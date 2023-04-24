#include <iostream>
#include <bits/stdc++.h>
#include <fstream>
#include <pthread.h>
//#include <cmath>
#include <math.h>
#include <map>

using namespace std;



struct structMapper
{
    int id;
    int mthreads;
    int rthreads;
    vector<string> file_names;
    int nr_thread_files;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;
    vector<int> *vizitat;
    map<int, vector<int>> mapa;
};

struct structReducer
{
    int id;
    int mthreads;
    int rthreads;
    structMapper *inputs_m;
    pthread_barrier_t *barrier;
};

int binarySearch(long long l, long long r, long  long exponent, long long numar)
{
    if (r >= l) {
        int mid = l + (r - l) / 2;
 
        if (pow(mid, exponent) == numar)
            return mid;

        if (pow(mid, exponent) > numar)
            return binarySearch(l, mid - 1, exponent, numar);
 
        return binarySearch(mid + 1, r, exponent, numar);
    }

    return -1;
}

void *f_map(void *arg)
{
    structMapper *data = (structMapper *)arg; 
    list<int> list_num;
    int num = 0;
    string line;

    for (int i = 0; i < data->file_names.size(); i++)
    {
        pthread_mutex_lock(data->mutex);

        if ((*data->vizitat)[i] == 0)
        {
            ifstream f_in(data->file_names[i]);
            (*data->vizitat)[i] = 1;

            pthread_mutex_unlock(data->mutex);

            getline(f_in, line); 
            list_num.clear();
            while (getline(f_in, line))
            {
                num = atoi(line.c_str());
                list_num.push_back(num); 
            }
            
            f_in.close();
           

            for (int exp = 2; exp <= data->rthreads + 1; exp++)
            {
                for (auto elem : list_num) 
                {
                    int base;
                    base = binarySearch(2, sqrt(elem), exp, elem);
                    if (elem == 1)
                    {
                        data->mapa[exp].push_back(elem);
                    }
                    else if (base != -1)
                    {
                        data->mapa[exp].push_back(elem);
                    }
                }
                    
            } 
        }
        else
        {
            pthread_mutex_unlock(data->mutex);
        }
    }

    pthread_barrier_wait(data->barrier);

    pthread_exit(NULL);
}

int countDistinct(vector<int> arr, int n)
{
    int res = 1;
    for (int i = 1; i < n; i++)
    {
        int j = 0;
        for (j = 0; j < i; j++)
            if (arr[i] == arr[j])
                break;
        if (i == j)
            res++;
    }
    return res;
}

void *f_reduce(void *arg)
{
    structReducer *data = (structReducer *)arg;    

    pthread_barrier_wait(data->barrier);

    map<int, vector<int>> rmap;

    for (int i = 0; i < data->mthreads; i++)
    {
        for (const auto &pair : data->inputs_m[i].mapa)
        {
            for (auto elem : pair.second)
            {
                rmap[pair.first].push_back(elem);
            }
        }
    }

    int counter;
    counter = countDistinct(rmap[data->id + 2], rmap[data->id + 2].size());

    // Scriere in fisiere out:
    string nr;
    nr = to_string(data->id + 2);
    string outfile_name = "out" + nr + ".txt";
    ofstream outfile(outfile_name);
    outfile << counter;
    outfile.close();

    pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    int NUM_THREADS;
    int mthreads = atoi(argv[1]);
    int rthreads = atoi(argv[2]);
    NUM_THREADS = rthreads + mthreads;

    pthread_mutex_t mutex;
    pthread_mutex_init(&mutex, NULL);

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, NUM_THREADS);

    // Citire din fisiere test.txt:
    ifstream fin(argv[3]);
    string line;
    getline(fin, line); // citeste nr de fisiere din test.txt
    int nr_files = atoi(line.c_str());
    vector<string> file_names;
    while (getline(fin, line))
    {
        file_names.push_back(line);
    }

    vector<int> vizitat; 
    for (int i = 0; i < nr_files; i++)
    {
        vizitat.push_back(0);
    }

    pthread_t m_threads[atoi(argv[1])];
    pthread_t r_threads[atoi(argv[2])];
    int r;
    void *status;
    long ids[NUM_THREADS];

    // Creare thread-uri:
    structMapper mapper_data[NUM_THREADS + 1];
    for (long thread_id = 0; thread_id < mthreads; thread_id++)
    {
        mapper_data[thread_id].id = thread_id;
        mapper_data[thread_id].mthreads = mthreads; // argv[1]
        mapper_data[thread_id].rthreads = rthreads; // argv[2]
        mapper_data[thread_id].file_names = file_names;
        mapper_data[thread_id].vizitat = &vizitat;
        mapper_data[thread_id].mutex = &mutex;
        mapper_data[thread_id].barrier = &barrier;

        r = pthread_create(&m_threads[thread_id], NULL, f_map, &mapper_data[thread_id]);

        if (r)
        {
            printf("Eroare la crearea thread-ului %ld\n", thread_id);
            exit(-1);
        }
    }

    vector<structReducer*> reducer_data;
    for (long thread_id = 0; thread_id < rthreads; thread_id++)
    {
        reducer_data.push_back(new structReducer);
        reducer_data[thread_id]->id = thread_id;
        reducer_data[thread_id]->mthreads = mthreads;
        reducer_data[thread_id]->rthreads = rthreads;
        reducer_data[thread_id]->inputs_m = mapper_data; // fiecare reducer are acces la mappersi
        reducer_data[thread_id]->barrier = &barrier;

        r = pthread_create(&r_threads[thread_id], NULL, f_reduce, reducer_data[thread_id]);

        if (r)
        {
            printf("Eroare la crearea thread-ului %ld\n", thread_id);
            exit(-1);
        }
    }

    for (long thread_id = 0; thread_id < mthreads; thread_id++)
    {
        r = pthread_join(m_threads[thread_id], &status);

        if (r)
        {
            printf("Eroare la asteptarea thread-ului %ld\n", thread_id);
            exit(-1);
        }
    }

    for (long thread_id = 0; thread_id < rthreads; thread_id++)
    {
        r = pthread_join(r_threads[thread_id], &status);

        if (r)
        {
            printf("Eroare la asteptarea thread-ului %ld\n", thread_id);
            exit(-1);
        }
    }

    pthread_mutex_destroy(&mutex);
    fin.close();
    pthread_exit(NULL);
}
