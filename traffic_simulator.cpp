#include <mpi.h>                // MPI library
#include <iostream>             // Input-output stream
#include <fstream>              // File stream
#include <vector>               // Vector container
#include <queue>                // Queue container
#include <thread>               // Thread library
#include <mutex>                // Mutex library
#include <condition_variable>   // Condition variable library
#include <chrono>               // Time library
#include <unordered_map>        // Unordered map container
#include <algorithm>            // Algorithm library

using namespace std;            // Namespace for standard library

// Define a struct to represent traffic signal data
struct TrafficRecord {
    string timestamp;           // Timestamp of the traffic data
    int lightId;                // ID of the traffic light
    int carsPassed;             // Number of cars passed during the timestamp
};

// Define a class for the bounded buffer (producer-consumer pattern)
class TrafficBuffer {
private:
    queue<TrafficRecord> buffer;    // Queue to store traffic data
    mutex mtx;                      // Mutex for mutual exclusion
    condition_variable cv;          // Condition variable for synchronization
    size_t capacity;                // Capacity of the buffer

public:
    TrafficBuffer(size_t cap) : capacity(cap) {}

    // Producer method to add traffic data to the buffer
    void addTrafficData(const TrafficRecord &data) {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this] { return buffer.size() < capacity; });
        buffer.push(data);
        cv.notify_all();
    }

    // Consumer method to consume traffic data from the buffer
    TrafficRecord getTrafficData() {
        unique_lock<mutex> lock(mtx);
        cv.wait(lock, [this] { return !buffer.empty(); });
        TrafficRecord data = buffer.front();
        buffer.pop();
        cv.notify_all();
        return data;
    }
};

// Function to process traffic data and report top congested traffic lights
void analyzeTrafficData(TrafficBuffer &buffer, int numLights) {
    unordered_map<int, int> congestionMap;
    int currentHour = -1;
    while (true) {
        TrafficRecord data = buffer.getTrafficData();
        congestionMap[data.lightId] += data.carsPassed;
        if (stoi(data.timestamp.substr(0, 2)) != currentHour) {
            currentHour = stoi(data.timestamp.substr(0, 2));
            cout << "Hour " << currentHour << " - Top " << numLights << " congested traffic lights:" << endl;
            vector<pair<int, int>> sortedLights(congestionMap.begin(), congestionMap.end());
            partial_sort(sortedLights.begin(), sortedLights.begin() + numLights, sortedLights.end(), [](const pair<int, int> &a, const pair<int, int> &b) {
                return a.second > b.second;
            });
            for (int i = 0; i < numLights && i < sortedLights.size(); ++i) {
                cout << "Traffic Light ID: " << sortedLights[i].first << ", Total Cars Passed: " << sortedLights[i].second << endl;
            }
            congestionMap.clear();
        }
    }
}

// Main function
int main(int argc, char** argv) {
    // Initialize MPI
    MPI_Init(NULL, NULL);
    int worldSize;
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
    int worldRank;
    MPI_Comm_rank(MPI_COMM_WORLD, &worldRank);

    // Define the filename for the traffic data file and the number of top congested traffic lights to report
    const string filename = "log.txt";
    const int numLights = 3;

    // Execute the following code block only by the master process
    if (worldRank == 0) {
        // Open the traffic data file
        ifstream file(filename);
        if (!file.is_open()) {
            cerr << "Error: Unable to open file: " << filename << endl;
            return 1;
        }

        // Read traffic data from the file and store it in a vector
        vector<TrafficRecord> trafficData;
        TrafficRecord temp;
        while (file >> temp.timestamp >> temp.lightId >> temp.carsPassed) {
            trafficData.push_back(temp);
        }
        file.close();

        // Create a bounded buffer for traffic data
        TrafficBuffer buffer(48);

        // Create a thread to consume traffic data and analyze congestion
        thread consumer(analyzeTrafficData, ref(buffer), numLights);

        // Simulate traffic data generation
        for (const auto &data : trafficData) {
            buffer.addTrafficData(data);
            this_thread::sleep_for(chrono::milliseconds(100));
        }

        // Join the consumer thread
        consumer.join();
    }

    // Finalize MPI
    MPI_Finalize();
    return 0;
}
