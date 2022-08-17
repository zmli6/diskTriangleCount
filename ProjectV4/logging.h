#include<iostream>
#include<sstream>
#include<string>
#include<vector>
#include<algorithm>
#include<map>
#include<fstream>
#include <stdio.h>
class logging
{
    private:
    bool log;
    string FilePath;
    string dataSetName;
    map<int,int>i_count;
    map<int,int>j_count;
    map<int,int>item_size;
    vector<int>degree;
    map<int,unsigned long long int>I_count;
    map<int,unsigned long long int>J_count;
    map<int,unsigned long long int>ITEM_size;
    public:
    void setLog(bool log){
        this->log = log;
    }
    bool getLog(){
        return this->log;
    }
    static bool cmp(const pair<int,int> &p1,const pair<int,int> &p2)//要用常数，不然编译错误 
    {
        return p1.second>p2.second;
    }
    void setDataSetName(string dataSetName){
        this->dataSetName = dataSetName;
    }
    string getFilePath(string op){
        return this->FilePath + "/" + op + ".txt";
    }
    void setFilePath(string filePath){
        this->FilePath = filePath;
    }
    void degreeSave(vector<int>degree){
        this->degree = degree;
        string degreePath = this->FilePath + "/degree/degree.txt";
        ofstream fp(degreePath);
        for(int i = 1; i < degree.size(); i++){
            fp << i << " " << degree[i] << endl;
        }
        fp.close();
    }
    void degreeGapSave(vector<int>degree){
        string degreePath = this->FilePath + "/degree/degreeGap.txt";
        ofstream fp(degreePath);
        for(int i = 0; i < degree.size(); i++){
            fp << i << " " << degree[i] << endl;
        }
        fp.close();
    }
    void setCFRecord(int src, int des, int size){
        //single Partition
        this->i_count[src]++;
        this->j_count[des]++;
        this->item_size[src] += 3 + size;
        this->item_size[des] += 3 + size;

        //all Partition
        this->I_count[src]++;
        this->J_count[des]++;
        this->ITEM_size[src] += 3 + size;
        this->ITEM_size[des] += 3 + size;
    }

    void writeParition(string pathIn, int PartitionID, int top){
        FILE * fp = fopen(pathIn.c_str(),"rb");
        int src, des, size ,temp;
        while(fread(&src ,sizeof(int), 1, fp) != 0){
            fread(&des ,sizeof(int), 1, fp);
            fread(&size ,sizeof(int), 1, fp);
            this->setCFRecord(src, des, size);
            while(size){
                fread(&temp ,sizeof(int), 1, fp);
                size--;
            }
        }
        string path = this->FilePath + "/companyFile/" + to_string(PartitionID) + "_" + to_string(top) + "_" +"data.txt";
        vector<pair<int,int> > arr;
        for (map<int,int>::iterator it = this->i_count.begin();it != this->i_count.end();++it)
        {
            arr.push_back(make_pair(it->first,it->second));
        }
        sort(arr.begin(),arr.end(),cmp);
        this->i_count.clear();
        ofstream fp_out(path, ios::out|ios::app);
        fp_out << "I item - ID companionFileTimes companionFileSize degree " << endl;
        int index = 0;int count = top;
        while(count && index < arr.size()){
            fp_out << arr[index].first << " " << arr[index].second << " " <<  this->item_size[arr[index].first] * 4 << " " << this->degree[arr[index].first] << endl;
            count--;
            index++;
        }
        fp_out << endl;
        arr.clear();
        for (map<int,int>::iterator it = this->j_count.begin();it != this->j_count.end();++it)
        {
            arr.push_back(make_pair(it->first,it->second));
        }
        sort(arr.begin(),arr.end(),cmp);
        this->j_count.clear();
        fp_out << "J item - ID companionFileTimes companionFileSize degree " << endl;
        index = 0; count = top;
        while(count && index < arr.size()){
            fp_out << arr[index].first << " " << arr[index].second << " " <<  this->item_size[arr[index].first] * 4 << " " << this->degree[arr[index].first] << endl;
            count--;
            index++;
        }
        fp_out << endl;
        arr.clear();
        fp_out.close();
        this->item_size.clear();
    }

    void writeParition(int top){
        string path = this->FilePath + "/PartitionFile/all_" + to_string(top) + "_" +"data.txt";
        vector<pair<int,unsigned long long int> > arr;
        for (map<int,unsigned long long int>::iterator it = this->I_count.begin();it != this->I_count.end();++it)
        {
            arr.push_back(make_pair(it->first,it->second));
        }
        sort(arr.begin(),arr.end(),cmp);
        this->I_count.clear();
        ofstream fp_out(path, ios::out|ios::app);
        fp_out << "I item - ID companionFileTimes companionFileSize degree " << endl;
        int index = 0;int count = top;
        while(count && index < arr.size()){
            fp_out << arr[index].first << " " << arr[index].second << " " <<  this->ITEM_size[arr[index].first] * 4 << " " << this->degree[arr[index].first] << endl;
            count--;
            index++;
        }
        fp_out << endl;
        arr.clear();
        for (map<int,unsigned long long int>::iterator it = this->J_count.begin();it != this->J_count.end();++it)
        {
            arr.push_back(make_pair(it->first,it->second));
        }
        sort(arr.begin(),arr.end(),cmp);
        this->J_count.clear();
        fp_out << "J item - ID companionFileTimes companionFileSize degree " << endl;
        index = 0; count = top;
        while(count && index < arr.size()){
            fp_out << arr[index].first << " " << arr[index].second << " " <<  this->ITEM_size[arr[index].first] * 4 << " " << this->degree[arr[index].first] << endl;
            count--;
            index++;
        }
        fp_out << endl;
        arr.clear();
        fp_out.close();
        this->ITEM_size.clear();
    }
};