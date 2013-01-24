#include <cstdlib>
#include <string.h>
#include <fstream>
#include <sstream>
#include <iostream>
#include <assert.h>
#include "stopWatch.h"
#include "pillowtalk.h"

using namespace std;

struct _data
{
    string key;
    string value;
};

struct _data2
{
    _data* tab;
    int number;
};

_data* allocate(int num, _data* T)
{
    T=new _data[num];
    return T;
}

_data* deallocate(_data* T)
{
    delete[] T;
    T=NULL;
    return T;
}

_data2* readFile(char* name, _data* T)
{
    fstream in(name, fstream::in);
    if(!in.is_open())
    {
        cerr << "Error with file";
        return NULL;
    }
    else
    {
        int num;
        string buffer;
        in >> num;
        T=allocate(num, T);
        for(int i=0;i<num;i++)
        {
            in >> T[i].key;
            in >> T[i].value;
        }
        in.close();
        _data2* T2=new _data2;
        T2->number=num;
        T2->tab=T;
        return T2;
    }
}

pt_node_t** prepare_data(pt_node_t** D, _data* T, int num)
{
    D=new pt_node_t*[num];
    for (int i=0; i<num; i++)
    {
        D[i] = pt_map_new();
        pt_map_set(D[i],"_id",pt_string_new(T[i].key.c_str()));
        pt_node_t* value = pt_map_new();
        pt_map_set(D[i],"value",value);
        pt_map_set(value,T[i].key.c_str(),pt_string_new(T[i].value.c_str()));
    }
    return D;
}

int main(int argc, char** argv)
{
    string host="s3thus.dyndns.org";
    string port="5984";
    string login="dawid";
    string pass="hardPass";
    string db="tester";
    string filename="1k";
    
    string config="http://";
    config.append(login);
    config.append(":");
    config.append(pass);
    config.append("@");
    config.append(host);
    config.append(":");
    config.append(port);
    config.append("/");
    config.append(db);
    string buf=config;
    
    stopWatch timer;
    
    _data* keys=NULL;
    _data2* b=NULL;
    int amount;
   
    /* Parse command line options. */
    argv++; argc--;
    while (argc)
    {
        if (argc >= 2 && !strcmp(argv[0],"-h"))
        {
            argv++; argc--;
            host = argv[0];
        }
        else if (argc >= 2 && !strcmp(argv[0],"-p"))
        {
            argv++; argc--;
            port = atoi(argv[0]);
        }
        else if (argc >= 2 && !strcmp(argv[0],"-f"))
        {
            argv++; argc--;
            filename = argv[0];
        }
        else if (argc >= 2 && !strcmp(argv[0],"-l"))
        {
            argv++; argc--;
            login = argv[0];
        }
        else if (argc >= 2 && !strcmp(argv[0],"-pass"))
        {
            argv++; argc--;
            pass = argv[0];
        }
        else if (argc >= 2 && !strcmp(argv[0],"-db"))
        {
            argv++; argc--;
            db = argv[0];
        }
        else
        {
            fprintf(stderr, "Invalid argument: %s\n", argv[0]);
            exit(1);
        }
        argv++; argc--;
    }
    
    b=readFile((char*)filename.c_str(),keys);
    keys=b->tab;
    amount=b->number;
    b=NULL;

    pt_init();
    pt_node_t** docs=NULL;
    
    docs=prepare_data(docs, keys, amount);

    pt_response_t* response = NULL;
    response = pt_delete(config.c_str());
    pt_free_response(response);
    response = pt_put(config.c_str(),NULL);
    pt_free_response(response);

    timer.startTimer();
    for(int i=0; i<amount; i++)
    {
        buf=config+'/'+keys[i].key;
        response = pt_put(buf.c_str(),docs[i]);
        assert(response->response_code == 201);
        pt_free_response(response);

        pt_free_node(docs[i]);
    }
    timer.stopTimer();
    cerr << "Task of " << amount << " PUT done in " << timer.getDuration() << "s.\n";
    pt_cleanup();
    delete[] docs;
    deallocate(keys);
}