# Table of Contents
1. [Approach](README.md#approach)
2. [Dependencies](README.md#dependencies)
3. [Run Instructions](README.md#run-nstructions)


# Approach
I made a class named user, containing id of the user, list of purchases in chronological order, and set of friends.
To build the network and users purchases I stored users in a dictionary with user id as their key. In order to avoid overhead of searching whether a user already exists in the dict or not I use exception handling to make the process more efficient.

When reading batch_log.json I only need to create users and update their purchase and friends list.
When reading stream_log.json I have to do the same I did with batch_log.json but also checking for anomalouse purchases on each purchase event. 

To find out anomalous purchases two functions were implemented:
1. find_social_network: finds social network of degree D of a specific person.
2. get_mean_sd: gathers last T purchases in a social network and returns the mean and standard deviation of them. If the list is less than 2 it provides 0, -1 for mean, sd as invalid.

The first function utilizes a BFS which only explores neighbours of level D and returns the id of users in that social network.
The second function finds the lates purchase among last purchases of each user in social net and adds it to the list and moves the index. It will try to find latest purchases of the social network in this manner until the size reaches T or the purchase history of each user gets exhausted.

Using these two function we can compare the purchase amount with the anomaly equiation and if it was greater it is flagged in written in output file right away. So the output data is keep getting appended while reading stream_log.

# Dependencies
In this task I used python 2.7 and it's built in packages. There is no need to install additional packages. 
Used json to convert json objects to python dict easily. Used sys to read arguments of log file names. And time to keep track of chronological order of same timestamps. Used sqrt for calculation of standard deviation.
# Run Instructions
As mentioned in your run file this command will do the process:
python ./src/process_log.py ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
process_log.py will read batch_log.json and build the social network. Then reads the stream_log.json and make updates while evaluating purchases for anomaly and writing them in flagged_purchases.json as output file.
The command provided in run.sh would