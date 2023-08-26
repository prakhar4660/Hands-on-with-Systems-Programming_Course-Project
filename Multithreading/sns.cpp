#include <bits/stdc++.h>
#include <pthread.h>
#include <time.h>
#include <random>
#include <unistd.h>
using namespace std;

#define NUM_NODES 37700
#define NUM_EDGES 289003

int rand_num(int a, int b)
{
    srand(time(NULL));
    return a + rand() % (b - a + 1);
}

typedef struct Action
{
    int user_id;
    int action_id;
    int action_type;
    time_t action_time;

    Action(int uid, int aid, int atype)
    {
        user_id = uid;
        action_id = aid;
        action_type = atype;
        time(&action_time);
    }
} Action;

typedef struct adj_list
{
    int id; // node_id
    int *neighbours;
    int num_neighbours;
    int count[3]; // {0: post, 1: comment, 2: like}
    queue<Action> wall_queue;
    queue<Action> feed_queue;
    int reading_order; // priority = 0 or chronological = 1
    pthread_mutex_t feed_lock;
    pthread_cond_t feed_cond;
    void insert_action_wall(Action new_action)
    {
        wall_queue.push(new_action);
    }
    void insert_action_feed(Action new_action)
    {
        feed_queue.push(new_action);
    }

    adj_list()
    {
        neighbours = NULL;
        reading_order = rand_num(0, 1);
        count[0] = 0;
        count[1] = 0;
        count[2] = 0;
        pthread_mutex_init(&feed_lock, NULL);
        pthread_cond_init(&feed_cond, NULL);
    }

    ~adj_list()
    {
        free(neighbours);
        pthread_mutex_destroy(&feed_lock);
        pthread_cond_destroy(&feed_cond);
    }

} adj_list;

adj_list nodes[NUM_NODES];
unordered_map<int, int> common_neighbours[10];

queue<Action> shared_q[25];
set<int> feed_q_set[10];

pthread_mutex_t logfile_lock, std_lock;
pthread_mutex_t shared_q_lock[25], feed_q_lock[10];
pthread_cond_t shared_q_cond[25], feed_q_cond[10];

void print(string s)
{
    ofstream file;
    file.open("sns.log", ios::app);
    if (!file.is_open())
    {
        cout << "Error opening log file" << endl;
        exit(1);
    }

    // Writing to logfile
    pthread_mutex_lock(&logfile_lock);
    file << s;
    pthread_mutex_unlock(&logfile_lock);

    // Writing to standard output
    pthread_mutex_lock(&std_lock);
    cout << s;
    pthread_mutex_unlock(&std_lock);

    file.close();
}

void file_read()
{
    // Creating a new file
    ofstream out;
    out.open("sns.log", ofstream::out | ofstream::trunc);
    out.close();

    int edges[NUM_EDGES][2];
    char line[50];
    int line_num = 0;
    FILE *f = fopen("musae_git_edges.csv", "r");
    if (f == NULL)
    {
        perror("error occured while opening the file");
        exit(1);
    }
    fgets(line, 50, f); // ignoring the first line of csv file
    while (fgets(line, 50, f))
    {
        int node1, node2;
        sscanf(line, "%d,%d", &node1, &node2);
        edges[line_num][0] = node1;
        edges[line_num][1] = node2;
        nodes[node1].id = node1;
        nodes[node1].num_neighbours++;
        nodes[node2].id = node2;
        nodes[node2].num_neighbours++;
        line_num++;
    }
    fclose(f);

    for (int i = 0; i < NUM_NODES; i++)
    {
        nodes[i].neighbours = (int *)malloc(nodes[i].num_neighbours * sizeof(int));
        nodes[i].num_neighbours = 0;
    }

    for (int i = 0; i < line_num; i++)
    {
        nodes[edges[i][0]].neighbours[nodes[edges[i][0]].num_neighbours] = edges[i][1];
        nodes[edges[i][0]].num_neighbours++;
        nodes[edges[i][1]].neighbours[nodes[edges[i][1]].num_neighbours] = edges[i][0];
        nodes[edges[i][1]].num_neighbours++;
    }

    // debugging: checking if all adjacency list are created or not

    ofstream file;
    file.open("Adj_list.txt", ios::out | ios::trunc);

    int cnt = 0;
    for (int i = 0; i < NUM_NODES; i++)
    {
        file << i << ": (" << nodes[i].num_neighbours << ")  --->  ";
        int n = 0; // number of neigbours
        for (int j = 0; j < nodes[i].num_neighbours; j++)
        {
            file << nodes[i].neighbours[j] << " ";
            cnt++;
            n++;
        }
        file << " : " << n << endl;
    }
    // printf("total edges: %d\n", cnt / 2);
}

void *user_simulator_thread(void *arg)
{
    while (1)
    {
        // random number generator
        mt19937 rng(time(nullptr));
        uniform_int_distribution<int> dist(0, NUM_NODES - 1);

        int i = 0;
        while (i < 100)
        {
            int node_i = dist(rng);
            // cout << node_i << " --> ";
            int degree = nodes[node_i].num_neighbours;
            int n = log2(degree) + 1; // proportional to log2(degree)

            string s = "";
            for (int j = 0; j < n; j++)
            {
                int action_type = rand() % 3; // {0: post, 1: comment, 2:like}
                nodes[node_i].count[action_type]++;

                Action *new_action = new Action(node_i, nodes[node_i].count[action_type], action_type);
                nodes[node_i].insert_action_wall(*new_action);

                int sq_id = rand_num(0, 24);

                pthread_mutex_lock(&shared_q_lock[sq_id]);
                shared_q[sq_id].push(*new_action); // Push action to push queue
                pthread_mutex_unlock(&shared_q_lock[sq_id]);
                pthread_cond_signal(&shared_q_cond[sq_id]);

                if (new_action->action_type == 0)
                    s += "User Simulator thread : Generated action '" + to_string(new_action->action_id) + "' of type 'post' posted by user '" + to_string(new_action->user_id) + "' at time " + ctime(&new_action->action_time);
                else if (new_action->action_type == 1)
                    s += "User Simulator thread : Generated action '" + to_string(new_action->action_id) + "' of type 'comment' posted by user '" + to_string(new_action->user_id) + "' at time " + ctime(&new_action->action_time);
                else
                    s += "User Simulator thread : Generated action '" + to_string(new_action->action_id) + "' of type 'like' posted by user '" + to_string(new_action->user_id) + "' at time " + ctime(&new_action->action_time);
            }
            print(s);
            // cout << endl;
            i++;
        }
        sleep(120);
    }
    pthread_exit(0);
}

int cmp_n;
bool mycmp1(Action a, Action b)
{
    return a.action_time < b.action_time;
}

bool mycmp0(Action a, Action b)
{
    return common_neighbours[cmp_n][a.user_id] > common_neighbours[cmp_n][b.user_id];
}

void *read_post_thread(void *arg)
{
    int n = (*(int *)arg);
    while (1)
    {
        pthread_mutex_lock(&feed_q_lock[n]);
        while(feed_q_set[n].empty())
            pthread_cond_wait(&feed_q_cond[n], &feed_q_lock[n]);
        auto it = feed_q_set[n].begin();
        int id = *it;
        feed_q_set[n].erase(it);
        pthread_mutex_unlock(&feed_q_lock[n]);

        vector<Action> feed_actions;
        int read_order, count = 0;
        pthread_mutex_lock(&nodes[id].feed_lock);
        while(nodes[id].feed_queue.empty())
            pthread_cond_wait(&nodes[id].feed_cond, &nodes[id].feed_lock);
        read_order = nodes[id].reading_order;
        while (nodes[id].feed_queue.size() > 0)
        {
            feed_actions.push_back(nodes[id].feed_queue.front());
            nodes[id].feed_queue.pop();
        }
        pthread_mutex_unlock(&nodes[id].feed_lock);

        // finding common neighbors between the poster node and reader node
        for (int i = 0; i < feed_actions.size(); i++)
        {
            int poster_id = feed_actions[i].user_id;
            set<int> st;
            for (int i = 0; i < nodes[poster_id].num_neighbours; i++)
            {
                st.insert(nodes[poster_id].neighbours[i]);
            }
            for (int i = 0; i < nodes[id].num_neighbours; i++)
            {
                if (st.count(nodes[id].neighbours[i]) != 0)
                {
                    count++;
                }
            }
            common_neighbours[n][poster_id] = count;
        }
        cmp_n = n;
        if (read_order == 1)
        {
            sort(feed_actions.begin(), feed_actions.end(), mycmp1);
        }
        else
        {
            sort(feed_actions.begin(), feed_actions.end(), mycmp0);
        }

        for (int i = 0; i < feed_actions.size(); i++)
        {
            string s;
            s = "readPost thread '" + to_string(n) + "' : I read action '" + to_string(feed_actions[i].action_id) + "' of type '" + (feed_actions[i].action_type == 0 ? "post" : feed_actions[i].action_type == 1 ? "comment"
                                                                                                                                                                                                                  : "like") +
                "' posted by user '" + to_string(feed_actions[i].user_id) + "' at time " + ctime(&feed_actions[i].action_time);
            print(s);
        }
    }
    pthread_exit(0);
}

void *push_update_thread(void *arg)
{
    // n-th push update thread
    int n = (*(int *)arg);
    while (1)
    {
        // Pop new action from shared queue
        pthread_mutex_lock(&shared_q_lock[n]);
        while(shared_q[n].empty())
            pthread_cond_wait(&shared_q_cond[n], &shared_q_lock[n]);
        Action action = shared_q[n].front();
        shared_q[n].pop();
        pthread_mutex_unlock(&shared_q_lock[n]);

        // Now pushing the action to feed queue of all neighbours
        for (int i = 0; i < nodes[(action.user_id)].num_neighbours; i++)
        {
            int neigh_id = nodes[(action.user_id)].neighbours[i];
            int feed_q_id = rand_num(0, 9);

            // Pushing to a particular neighbours's feed queue
            pthread_mutex_lock(&nodes[neigh_id].feed_lock);
            nodes[neigh_id].feed_queue.push(action);
            pthread_mutex_unlock(&nodes[neigh_id].feed_lock);
            pthread_cond_signal(&nodes[neigh_id].feed_cond);

            // Storing Unique neighbours for future use by readPost threads
            pthread_mutex_lock(&feed_q_lock[feed_q_id]);
            feed_q_set[feed_q_id].insert(neigh_id);
            pthread_mutex_unlock(&feed_q_lock[feed_q_id]);
            pthread_cond_signal(&feed_q_cond[feed_q_id]);

            string s;
            string tmp = ctime(&action.action_time);
            tmp.pop_back();
            if (action.action_type == 0)
                s = "pushUpdate thread '" + to_string(n) + "' : pushed action '" + to_string(action.action_id) + "' of type 'post' posted by user '" + to_string(action.user_id) + "' at time " + tmp + "' to neighbour '"+ to_string(neigh_id) + "'\n";
            else if (action.action_type == 1)
                s = "pushUpdate thread '" + to_string(n) + "' : pushed action '" + to_string(action.action_id) + "' of type 'comment' posted by user '" + to_string(action.user_id) + "' at time " + tmp + "' to neighbour '"+ to_string(neigh_id) + "'\n";
            else
                s = "pushUpdate thread '" + to_string(n) + "' : pushed action '" + to_string(action.action_id) + "' of type 'like' posted by user '" + to_string(action.user_id) + "' at time " + tmp + "' to neighbour '"+ to_string(neigh_id) + "'\n";

            print(s);
        }
    }
    pthread_exit(0);
}

int main()
{
    // Initialize mutexes for log file and stdout and shared queues
    pthread_mutex_init(&logfile_lock, NULL);
    pthread_mutex_init(&std_lock, NULL);

    for (int i = 0; i < 25; i++)
    {
        pthread_mutex_init(&shared_q_lock[i], NULL);
    }
    for (int i = 0; i < 10; i++)
    {
        pthread_mutex_init(&feed_q_lock[i], NULL);
    }

    // Initialize all the condition variables
    for (int i = 0; i < 10; i++)
        pthread_cond_init(&feed_q_cond[i], NULL);
    for (int i = 0; i < 25; i++)
        pthread_cond_init(&shared_q_cond[i], NULL);

    file_read();

    // Creating the userSimulator thread
    pthread_t userSimulator;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&userSimulator, &attr, user_simulator_thread, NULL);

    // Creating 25 pushUpdate threads
    pthread_t pushUpdate[25];
    int pushUpdate_num[25];
    for (int j = 0; j < 25; j++)
    {
        pushUpdate_num[j] = j;
        pthread_create(&pushUpdate[j], NULL, push_update_thread, &pushUpdate_num[j]);
    }

    // Creating 10 readPost threads
    pthread_t readPost[10];
    int readPost_num[10];
    for (int j = 0; j < 10; j++)
    {
        readPost_num[j] = j;
        pthread_create(&readPost[j], NULL, read_post_thread, &readPost_num[j]);
    }

    // Waiting for the threads to finish
    pthread_join(userSimulator, NULL);
    for (int j = 0; j < 25; j++)
    {
        pthread_join(pushUpdate[j], NULL);
    }
    for (int j = 0; j < 10; j++)
    {
        pthread_join(readPost[j], NULL);
    }

    // destroying mutexes
    pthread_mutex_destroy(&logfile_lock);
    pthread_mutex_destroy(&std_lock);
    for (int i = 0; i < 25; i++)
    {
        pthread_mutex_destroy(&shared_q_lock[i]);
    }
    for (int i = 0; i < 10; i++)
    {
        pthread_mutex_destroy(&feed_q_lock[i]);
    }

    // destroying condition variables
    for (int i = 0; i < 25; i++)
        pthread_cond_destroy(&shared_q_cond[i]);
    for (int i = 0; i < 10; i++)
        pthread_cond_destroy(&feed_q_cond[i]);

    return 0;
}