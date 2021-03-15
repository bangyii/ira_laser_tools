#include <ros/ros.h>
#include <string>
#include <iostream>
#include <tf/transform_listener.h>
#include <pcl_ros/transforms.h>
#include <laser_geometry/laser_geometry.h>
#include <pcl/conversions.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl/io/pcd_io.h>
#include <sensor_msgs/PointCloud.h>
#include <sensor_msgs/PointCloud2.h>
#include <sensor_msgs/point_cloud_conversion.h>
#include "sensor_msgs/LaserScan.h"
#include "pcl_ros/point_cloud.h"
#include <Eigen/Dense>
#include <dynamic_reconfigure/server.h>
#include <ira_laser_tools/laserscan_multi_mergerConfig.h>

using namespace std;
using namespace pcl;
using namespace laserscan_multi_merger;

class LaserscanMerger
{
public:
	LaserscanMerger();
	void scanCallback(const sensor_msgs::LaserScan::ConstPtr &scan, std::string topic);
	void timerCallback(const ros::TimerEvent &);
	void pointcloud_to_laserscan(Eigen::MatrixXf points, pcl::PCLPointCloud2 *merged_cloud);
	void reconfigureCallback(laserscan_multi_mergerConfig &config, uint32_t level);

private:
	ros::NodeHandle node_;
	ros::Timer timer;
	laser_geometry::LaserProjection projector_;
	tf::TransformListener tfListener_;

	ros::Publisher point_cloud_publisher_;
	ros::Publisher laser_scan_publisher_;
	vector<ros::Subscriber> scan_subscribers;

	vector<pcl::PCLPointCloud2> clouds;
	vector<string> input_topics;
	vector<string> unsubscribed_topics;

	void laserscan_topic_parser();
	bool subscribeTopic(const std::string &topic, std::vector<ros::Subscriber> &subscribers);
	void subscribeAllTopics(std::vector<std::string> &topics);

	double angle_min;
	double angle_max;
	double angle_increment;
	double time_increment;
	double scan_time;
	double range_min;
	double range_max;
	bool publish_pointcloud = false;

	string destination_frame;
	string cloud_destination_topic;
	string scan_destination_topic;
	string laserscan_topics;
};

void LaserscanMerger::reconfigureCallback(laserscan_multi_mergerConfig &config, uint32_t level)
{
	this->angle_min = config.angle_min;
	this->angle_max = config.angle_max;
	this->angle_increment = config.angle_increment;
	this->time_increment = config.time_increment;
	this->scan_time = config.scan_time;
	this->range_min = config.range_min;
	this->range_max = config.range_max;
}

bool LaserscanMerger::subscribeTopic(const std::string &topic, std::vector<ros::Subscriber> &subscribers)
{
	//Get available topics from ROS master
	ros::master::V_TopicInfo topics;
	ros::master::getTopics(topics);

	//Check if topic to be subscribed exists in list of ros master topics
	auto it = std::find_if(topics.begin(), topics.end(), [&](const ros::master::TopicInfo &t) {
		return t.name.compare(topic) == 0 && t.datatype.compare("sensor_msgs/LaserScan") == 0;
	});

	//Subscribe, matching topic found
	if (it != topics.end())
	{
		subscribers.push_back(node_.subscribe<sensor_msgs::LaserScan>(topic, 1, boost::bind(&LaserscanMerger::scanCallback, this, _1, topic)));
		return true;
	}

	else
		return false;
}

void LaserscanMerger::subscribeAllTopics(std::vector<std::string> &topics)
{
	for (int i = 0; i < topics.size(); ++i)
	{
		if (subscribeTopic(topics[i], scan_subscribers))
		{
			ROS_INFO("Laser Scan Merger subscribed to %s", topics[i].c_str());

			//Add topic to list of subscribed topics
			input_topics.push_back(topics[i]);

			//Remove the topic from unsubscribed topics on successful subscription
			topics.erase(topics.begin() + i);
			--i;
		}
	}

	//Resize cloud to fit all subscribed topics
	clouds.resize(scan_subscribers.size());
}

void LaserscanMerger::timerCallback(const ros::TimerEvent &)
{
	subscribeAllTopics(unsubscribed_topics);
	if (unsubscribed_topics.empty())
		timer.stop();
}

void LaserscanMerger::laserscan_topic_parser()
{
	istringstream iss(laserscan_topics);
	copy(istream_iterator<string>(iss), istream_iterator<string>(), back_inserter<vector<string>>(unsubscribed_topics));

	//Remove duplicate topics from param file if any
	sort(unsubscribed_topics.begin(), unsubscribed_topics.end());
	auto last = std::unique(unsubscribed_topics.begin(), unsubscribed_topics.end());
	unsubscribed_topics.erase(last, unsubscribed_topics.end());

	// Unsubscribe from previous topics
	for (int i = 0; i < scan_subscribers.size(); ++i)
		scan_subscribers[i].shutdown();

	// Subscribe to topics if they exist, else store in queue to try again later
	if (!unsubscribed_topics.empty())
		subscribeAllTopics(unsubscribed_topics);

	else
		ROS_INFO("Not subscribed to any topic.");
}

LaserscanMerger::LaserscanMerger()
{
	ros::NodeHandle nh("~");

	nh.param<std::string>("destination_frame", destination_frame, "cart_frame");
	nh.param<std::string>("cloud_destination_topic", cloud_destination_topic, "/merged_cloud");
	nh.param<std::string>("scan_destination_topic", scan_destination_topic, "/scan_multi");
	nh.param<std::string>("laserscan_topics", laserscan_topics, "");
	nh.param("angle_min", angle_min, -2.36);
	nh.param("angle_max", angle_max, 2.36);
	nh.param("angle_increment", angle_increment, 0.0058);
	nh.param("scan_time", scan_time, 0.0333333);
	nh.param("range_min", range_min, 0.45);
	nh.param("range_max", range_max, 25.0);
	nh.param("publish_pointcloud", publish_pointcloud, false);

	this->laserscan_topic_parser();

	//Create timer event to attempt susbcribing topics if some were left unsubscribed
	if (!unsubscribed_topics.empty())
		timer = node_.createTimer(ros::Duration(0.1), &LaserscanMerger::timerCallback, this);

	if (publish_pointcloud)
		point_cloud_publisher_ = node_.advertise<sensor_msgs::PointCloud2>(cloud_destination_topic.c_str(), 1, false);

	laser_scan_publisher_ = node_.advertise<sensor_msgs::LaserScan>(scan_destination_topic.c_str(), 1, false);
}

void LaserscanMerger::scanCallback(const sensor_msgs::LaserScan::ConstPtr &scan, std::string topic)
{
	sensor_msgs::PointCloud tmpCloud1, tmpCloud2;
	sensor_msgs::PointCloud2 tmpCloud3;

	// Verify that TF knows how to transform from the received scan to the destination scan frame
	tfListener_.waitForTransform(scan->header.frame_id.c_str(), destination_frame.c_str(), scan->header.stamp, ros::Duration(1));
	projector_.transformLaserScanToPointCloud(scan->header.frame_id, *scan, tmpCloud1, tfListener_, laser_geometry::channel_option::Distance);
	try
	{
		tfListener_.transformPointCloud(destination_frame.c_str(), tmpCloud1, tmpCloud2);
	}
	catch (tf::TransformException ex)
	{
		ROS_ERROR("%s", ex.what());
		return;
	}

	//Fill cloud to respective index location
	for (int i = 0; i < input_topics.size(); ++i)
	{
		if (topic.compare(input_topics[i]) == 0)
		{
			sensor_msgs::convertPointCloudToPointCloud2(tmpCloud2, tmpCloud3);
			pcl_conversions::toPCL(tmpCloud3, clouds[i]);
		}
	}

	//Count number of valid scans
	int valid_scans = 0;
	for(const auto & cloud : clouds)
	{
		if(cloud.header.frame_id != "")
			valid_scans++;
	}

	// int first_non_empty = std::distance(clouds.begin(), find_if(clouds.begin(), clouds.end(), [](const pcl::PCLPointCloud2 &x) { return !x.data.empty(); }));
	// if (first_non_empty < clouds.size())
	if(valid_scans == clouds.size())
	{
		pcl::PCLPointCloud2 merged_cloud = clouds[0];
		for (int i = 1; i < clouds.size(); ++i)
		{
			pcl::concatenatePointCloud(merged_cloud, clouds[i], merged_cloud);
			clouds[i].header.frame_id = "";
		}

		if (publish_pointcloud)
			point_cloud_publisher_.publish(merged_cloud);

		Eigen::MatrixXf points;
		getPointCloudAsEigen(merged_cloud, points);

		pointcloud_to_laserscan(points, &merged_cloud);
	}
}

void LaserscanMerger::pointcloud_to_laserscan(Eigen::MatrixXf points, pcl::PCLPointCloud2 *merged_cloud)
{
	sensor_msgs::LaserScanPtr output(new sensor_msgs::LaserScan());
	output->header = pcl_conversions::fromPCL(merged_cloud->header);
	output->header.frame_id = destination_frame.c_str();
	output->header.stamp = ros::Time::now(); //fixes #265
	output->angle_min = this->angle_min;
	output->angle_max = this->angle_max;
	output->angle_increment = this->angle_increment;
	output->time_increment = this->time_increment;
	output->scan_time = this->scan_time;
	output->range_min = this->range_min;
	output->range_max = this->range_max;

	uint32_t ranges_size = std::ceil((output->angle_max - output->angle_min) / output->angle_increment);
	output->ranges.assign(ranges_size, output->range_max + 1.0);

	for (int i = 0; i < points.cols(); i++)
	{
		const float &x = points(0, i);
		const float &y = points(1, i);
		const float &z = points(2, i);

		if (std::isnan(x) || std::isnan(y) || std::isnan(z))
		{
			ROS_DEBUG("rejected for nan in point(%f, %f, %f)\n", x, y, z);
			continue;
		}

		double range_sq = y * y + x * x;
		double range_min_sq_ = output->range_min * output->range_min;
		if (range_sq < range_min_sq_)
		{
			ROS_DEBUG("rejected for range %f below minimum value %f. Point: (%f, %f, %f)", range_sq, range_min_sq_, x, y, z);
			continue;
		}

		double angle = atan2(y, x);
		if (angle < output->angle_min || angle > output->angle_max)
		{
			ROS_DEBUG("rejected for angle %f not in range (%f, %f)\n", angle, output->angle_min, output->angle_max);
			continue;
		}
		int index = (angle - output->angle_min) / output->angle_increment;

		if (output->ranges[index] * output->ranges[index] > range_sq)
			output->ranges[index] = sqrt(range_sq);
	}

	laser_scan_publisher_.publish(output);
}

int main(int argc, char **argv)
{
	ros::init(argc, argv, "laser_multi_merger");

	LaserscanMerger _laser_merger;

	dynamic_reconfigure::Server<laserscan_multi_mergerConfig> server;
	dynamic_reconfigure::Server<laserscan_multi_mergerConfig>::CallbackType f;

	f = boost::bind(&LaserscanMerger::reconfigureCallback, &_laser_merger, _1, _2);
	server.setCallback(f);

	ros::spin();

	return 0;
}
