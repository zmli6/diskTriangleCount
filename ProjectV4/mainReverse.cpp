#include<iostream>
#include<string>
#include<vector>
#include<fstream>
#include <cstring>
#include<map>
#include <unordered_map>
#include <math.h>
#include<algorithm>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
#include<climits>
#include<bitset>
#include<time.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include<cassert>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <omp.h>
#include <set>
#include <malloc.h>
#include "BBF.h"
using namespace std;
enum OpenMode {
    READ,
    WRITE
};
struct idDegree{
    int degree = 0;
    int id;
};
class CFTriple {
public:
	int i;
	int j;
	vector<int> neighborList;
};

class Edge {
public:
	int s;
	int d;
    void set(int _s, int _d) {
        s = _s;
        d = _d;
    }
};

// int loadBatch(File* _fp, int* _buf_pair, int _buf_size) {
//     string _buf;
//     int s = 0, d = 0;
//     for(int i = 0; i < _buf_size; i ++) {
//         getline(_fp, _buf);
//         if(_buf.size() < 1) {
//             return i;
//         }
//         stringstream _ss;
//         _ss << _buf;
//         _ss >> s >> d;
//         _buf_pair[i*2] = s;
//         _buf_pair[i*2+1] = d;
//     }
//     return _buf_size;
// }

int loadBatch(ifstream& _fin, int* _buf_pair, int _buf_size) {
    string _buf;
    int s = 0, d = 0;
    for(int i = 0; i < _buf_size; i ++) {
        getline(_fin, _buf);
        if(_buf.size() < 1) {
            return i;
        }
        stringstream _ss;
        _ss << _buf;
        _ss >> s >> d;
        //cout << "loadBatch" << s << " " << d << endl;
        _buf_pair[i*2] = s;
        _buf_pair[i*2+1] = d;
    }
    return _buf_size;
}

class CompanionFile {
public:
	FILE * fp;
	string filename;
	int partitionID;
    unsigned long long int fileSize;
    unsigned long long int compaionItemNum;

    bool exists_test3 (const std::string& name) {
        struct stat buffer;   
        return (stat (name.c_str(), &buffer) == 0); 
    }
	bool Open(OpenMode openMode,bool create) {
        if(create == true){
            if (openMode==READ) {
                fp = fopen(filename.c_str(), "rb");
                return true;
			} else if (openMode==WRITE) {
                fp = fopen(filename.c_str(), "ab");
                return true;
			}
            else{
                return false;
            }
        }
        else {
            if(exists_test3(filename) == true){
                if (openMode==READ) {
                    fp = fopen(filename.c_str(), "rb");
                    return true;
                } else if (openMode==WRITE) {
                    fp = fopen(filename.c_str(), "ab");
                    return true;
                }
                else{
                    return false;
                }
            }
            else{
                return false;
            }
        }
	}

	CFTriple readNext() {
        CFTriple temp;
        int i, j;
        if(fread(&i, sizeof(int), 1,fp)==0){
            temp.i = -1;
            return temp;
        }
        fread(&j, sizeof(int), 1,fp);
        int size;
        fread(&size, sizeof(int), 1,fp);
        vector<int>neighborList;
        for(int i = 0; i < size; i++){
            int temp1;
            fread(&temp1, sizeof(int), 1,fp);
            neighborList.push_back(temp1);
        }
        temp.neighborList = neighborList;
        temp.i = i;
        temp.j = j;
        return temp;
	}

	bool Write(CFTriple& cfTriple) {
        int i = cfTriple.i;
        int j = cfTriple.j;
        fwrite(&i, sizeof(int), 1,fp);
        fwrite(&j, sizeof(int), 1,fp);
        int size = cfTriple.neighborList.size();
        fwrite(&size, sizeof(int), 1,fp);
        for(int i = 0; i < size; i++){
            int temp = cfTriple.neighborList[i];
            fwrite(&temp, sizeof(int), 1,fp);
        }
        this->compaionItemNum++;
        return true;
	}

	bool hasNext() {
        if(feof(fp) == 0) return true;
        return false;
	}

	bool Close() {
        fclose(fp);
        return true;
	}
};

class Partition {
public:
	Partition(int _id, int _begin, int _end) {
		edgeInMemory = NULL;
        id = _id;
        begin = _begin;
        end = _end;
        edgeNum = 0;
	}

	map<int, vector<int>> LoadAdj(FILE * fp) {
        map<int, vector<int>>allVertexAdj;
        int src ,des;
        //cout << "Load in index " << id  << " j is " << j << endl;
        vector<int>j_adj;
        int preSrc;
        while(fread(&src ,sizeof(int), 1,fp)){
            fread(&des ,sizeof(int), 1,fp);
            auto iter = allVertexAdj.find(src);
            if(iter != allVertexAdj.end()){
                allVertexAdj[src].push_back(des);
            }
            else{
                vector<int>temp;
                temp.push_back(des);
                allVertexAdj[src] = temp;
            }
        }
        return allVertexAdj;
	}

	void Release() {
		delete [] edgeInMemory;
		edgeInMemory = NULL;
	}

	int id;
	int begin;
	int end;
	int edgeNum;
	CompanionFile cfile;

private:
	Edge* edgeInMemory;
};
class Dataset {
public:
	string datName;
	string pathDat;
	int edgeNum;
	int maxVertexID;
	int partitionNum;
	int partitionSize;
    int mergeSortMaxEdge;
    unsigned long long int triangle;
    unsigned long long int fileSize;
	string sortedBySrcPathDat;
	string partition;
    string companionFilePath;
    string newEdgeFile;
    string newEdgeFileBackup;
    string partitionDir;
    string dataPath;
    string tempSortFilePrefix;
    string tempFile;
    string OldID2NewIDFile;
    string newEdgeFileTxt;
    int * id2Degree;
    vector<int>index2Degree;
	vector<int>partitionGap;
	vector<Partition> partitionList;
    vector<unsigned long long int>cfNum;
    vector<bitset<8>>srcInPartitionList;
    BBF bbf;
    static bool cmp(idDegree  a, idDegree  b){
        if(a.degree > b.degree){
            return true;
        }
        else if (a.degree == b.degree){
            return a.id < b.id;
        }
        else return false;
    }
	bool preprocess() {
        
		// relabeling
				// ID-remapping
                IDMapping();
                cout << "IDMapping finish" << endl;
				// build newID2degree;
				// partition gap;
                buildPartitionGap();
                cout << "PartitionGap finish" << endl;
				// IDasSrcPartList;
				// buildPartitionFile
                buildPartitionFile();
                cout << "buildPartitionFile finish" << endl;
                return true;
		// 
	}

	bool buildCompanionFile(){
		// open companion file in write mode
		//sequentially edgeTable on disk;
        FILE * fp = fopen(this->newEdgeFile.c_str(), "rb");
        int src,des;
        fread(&src, sizeof(int), 1, fp);
        fread(&des, sizeof(int), 1, fp);
        for(int i = 1; i <= maxVertexID; i++){
            if(i % 100000 == 0)cout << "write companion file index: " << i << endl;
            vector<int>i_adj;
            while(i == src){
                i_adj.push_back(des);
                if(fread(&src, sizeof(int), 1, fp) == 0) break;
                fread(&des, sizeof(int), 1, fp);
            }//获取i的邻接表
            // cout << "i_adj iIndex is " << i << endl;
            // for(auto i :i_adj){
            //     cout << i << " ";
            // }
            // cout << endl;
            for(int adj_index = 0; adj_index < i_adj.size(); adj_index++){
                int j = i_adj[adj_index];//从i的邻接表出发
                if(j > i){// j 大于 i
                    //set<int>J_asSrcInPartitionList = this->IDasSrcInPartitionList[j];
                    bitset<8>J_asSrcInPartitionList = srcInPartitionList[j];
                    for(int J_asSrcPartitionIndex = 0; J_asSrcPartitionIndex <= this->partitionNum + 1 ; J_asSrcPartitionIndex++){
                        if(J_asSrcInPartitionList[J_asSrcPartitionIndex] == true){
                            Partition partition = this->partitionList[J_asSrcPartitionIndex];//获取分区Partition
                            // cout << "J is " << j << endl;
                            // cout << "partition index: " << J_asSrcPartitionIndex << " partition begin : " << partition.begin << " partition end: " << partition.end << endl;
                            vector<int>i_adj_copy;
                            for(int i = 0; i< i_adj.size(); i++){
                                if(i_adj[i] > j && partition.begin <= i_adj[i]&& i_adj[i] <= partition.end ){//第三点大于j 且在区间内
                                    i_adj_copy.push_back(i_adj[i]);
                                    //cout << "success " << i_adj[i] << endl;
                                }
                            }
                            
                            //vend 此处j 和 i_adj_copy 做一个batchVendTest
                            //将i中符合Partition begin 和 end 的元素加入到 i_adj_copy
                            if(i_adj_copy.size() != 0){
                                if(this->bbf.NEpairTest(j, i_adj_copy) == false){//若不为0
                                    //cout << "write companin file" << endl;
                                    string txt = "/media/data/demoTC/compangFileRe/" + to_string(J_asSrcPartitionIndex) + ".txt";
                                    ofstream fp_txt(txt, ios::out|ios::app);
                                    CFTriple cftriple;
                                    cftriple.i = i;
                                    cftriple.j = j;
                                    cftriple.neighborList = i_adj_copy;
                                    fp_txt << i << " " << j << " " << i_adj_copy.size() << " ";
                                    for(auto i : i_adj_copy){
                                        fp_txt << i << " ";
                                    }
                                    fp_txt << endl;
                                    partition.cfile.Open(WRITE,true);
                                    partition.cfile.Write(cftriple);//写入cfile
                                    cfNum[J_asSrcPartitionIndex]++;
                                    partition.cfile.Close();
                                }
                                // else{
                                //     cout << "NEpairTest success " << endl;
                                //     cout << endl;
                                // }
                            }
                            // else{
                            //     cout << "i_adj_copy is zero" << endl;
                            //     cout << endl;
                            // }
                        }
                        
                    
                }
                }
            }
            
        }
        fclose(fp);
        cout << "finsh companion file" << endl;
        return true;
	}

	int triangleCount() {
		for(int partitionI = 0; partitionI < this->partitionList.size(); partitionI++){
            //read all vertex's adj in this paritition
            string path = partitionDir + to_string(partitionI) + ".dat";
            if(this->partitionList[partitionI].cfile.Open(READ,false)==true){
                FILE * fp = fopen(path.c_str(),"rb");
                cout << "partition ID is " << partitionI << endl;
                map<int, vector<int>> partitionAdj = this->partitionList[partitionI].LoadAdj(fp);
                // for(auto test = partitionAdj.begin(); test != partitionAdj.end(); test ++){
                //     cout << "j: " << test->first << endl;
                //     for(auto test_1 : test->second){
                //         cout << test_1 << " ";
                //     }
                //     cout << endl;
                // }
                // cout << endl;
                fclose(fp); 
                int test_count = 0;
                if(this->partitionList[partitionI].cfile.fp != NULL){//该partition file 是否被创建
                    int i, j, size;
                    while(this->partitionList[partitionI].cfile.hasNext()){//如果有下一个cftripe
                        if(test_count % 10000 == 0)cout <<  "triangleCount compaion file partitionI: " << partitionI << "read cftriple "<< test_count <<endl;
                        CFTriple cftriple = this->partitionList[partitionI].cfile.readNext();//读取cftripe
                        if(cftriple.i == -1)break;
                        vector<int>thisPartitionAdjOfJ = partitionAdj[cftriple.j];
                        vector<int>intersec;
                        // cout << "i: " << cftriple.i << " j: " << cftriple.j << endl; 
                        // cout << "cftriple.neighborList" << endl;
                        // for(auto i : cftriple.neighborList){
                        //     cout << i << " ";
                        // }
                        // cout << endl;
                        // cout << "thisPartitionAdjOfJ j: "<< endl;
                        // for(auto i :thisPartitionAdjOfJ){
                        //     cout << i << " ";
                        // }
                        // cout << endl;
                        for(int i = 0; i < thisPartitionAdjOfJ.size(); i++){
                            if(find(cftriple.neighborList.begin(), cftriple.neighborList.end(),thisPartitionAdjOfJ[i])!=cftriple.neighborList.end()){
                                //cout << "success : "<<cftriple.i << " " << cftriple.j << " " << thisPartitionAdjOfJ[i] << endl;
                                this->triangle++;
                            }
                        }
                        // std::set_intersection(cftriple.neighborList.begin(), cftriple.neighborList.end(),
                        //             thisPartitionAdjOfJ.begin(), thisPartitionAdjOfJ.end(),
                        //             std::back_inserter(intersec));//交集
                        // if(intersec.size() != 0){
                        //     cout << "success item size is " << intersec.size() << endl;
                        //     for(auto item : intersec){
                        //         cout << cftriple.i << " " << cftriple.j << " " << item << endl;
                        //     }
                        //     cout << endl;
                        // }
                        // this->triangle += intersec.size();
                        test_count++;
                    }
                }
                partitionList[partitionI].cfile.Close();
            }
            //cout << "companion file index: " << partitionI << " triangle num " << this->triangle << endl;
        }
        return triangle;
	}

    bool writeCompanionFileNum(ofstream& fp){
        unsigned long long int total = 0;
        for(int i = 0; i < partitionList.size(); i++){
            total += cfNum[i];
            fp << "partition ID: " << to_string(i) << " companion file num is: " << to_string(cfNum[i]) << endl; 
        }
        fp << "avg companion file num is: " << total / this-> partitionList.size() << endl; 
        return true;
    }
private:
    int getOldID2Degree(){
        cout << "begin IDMapping" << endl;
		//build prevId2prevDegree;
        ifstream fp(this->dataPath, ios::in);
        string temp;
        int _maxOldID = 0; 
        this -> id2Degree =new int [this->maxVertexID + 10]();//init id2Degree
        while(getline(fp,temp)){
            if(temp.at(0) > '9' || temp.at(0) < '0' || temp.length() < 1) {
                break;
            }
            stringstream _ss;
            int src, des;
            _ss << temp;
            _ss >> src >> des;
            _maxOldID = (_maxOldID < src) ? src : _maxOldID;
            _maxOldID = (_maxOldID < des) ? des : _maxOldID;
            if(src < des){
                this->id2Degree[src] ++;
                this->id2Degree[des] ++;
            }
            
        }
        fp.close();
        return _maxOldID;
    }
    void idCover2NewEdgeFile(int _maxOldID){
        cout << "begin degree sort" << endl;
        vector<idDegree>sortVector;
        for (int i = 1; i <= _maxOldID; ++i)
        {
           idDegree temp;
           temp.degree = this -> id2Degree[i];
           temp.id = i;
           sortVector.push_back(temp);
        }
        sort(sortVector.begin(), sortVector.end(), cmp);
        cout << "degree sort end" << endl;
        delete this -> id2Degree;

        this->index2Degree.clear();
        int* pos2newID = new int[_maxOldID+1];//oldID -> newID
        int idCurrent = 1;//newID begin from 1
        for (auto iter = sortVector.begin(); iter != sortVector.end(); iter ++) {
            pos2newID[iter->id] = idCurrent;
            this->index2Degree.push_back(iter->degree);//index(new ID) -> degree
            idCurrent ++;
        }

		// sequentially read prev edge file and output new edge into new edgefile
        ifstream fp_oriEdgeFile(this->dataPath, ios::in);
        FILE * fp_NewIDEdgeFileBackup = fopen(this->newEdgeFileBackup.c_str(), "wb");
        ftruncate(fileno(fp_NewIDEdgeFileBackup), this->fileSize);
        if(fp_NewIDEdgeFileBackup == NULL){
            cout << "error open fp_NewIDEdgeFileBackup" << endl;
            abort();
        }
        string _buf;
        string txt = "/media/data/demoTC/newGraphRe/newEdgeBackup.txt";
        ofstream fp_txt(txt, ios::out);
        while(getline(fp_oriEdgeFile, _buf)){
            if(_buf.size() < 1) {
                break;
            }
            stringstream _ss;
            _ss << _buf;
            int s = 0, d = 0;
            _ss >> s >> d;
            int newS = pos2newID[s];
            int newD = pos2newID[d];
            fwrite(&newS, sizeof(int), 1, fp_NewIDEdgeFileBackup);
            fwrite(&newD, sizeof(int), 1, fp_NewIDEdgeFileBackup);
            fp_txt << newS << " " << newD << endl;
        }
        fclose(fp_NewIDEdgeFileBackup);
        fp_oriEdgeFile.close();
        fp_txt.close();
        delete [] pos2newID;
    }
    void mergeSort(){
        //revere prevDegree2prevID;
        //sort all the ID by degree
        cout<< "begin MergeSort" << endl;
		// sequentially read new edgefile and write into bNum sorted blocks
        FILE * fp_newEdgeBackup = fopen(this->newEdgeFileBackup.c_str(), "rb");
        if(fp_newEdgeBackup == NULL){
            cout << "write into bNum blocks open file error " << this->newEdgeFileBackup.c_str() << endl;
            abort();
        }
        multimap<int, int> blockEdgeSorted;
        int edgeCount = 0;
        int bNum = 0;
        vector<int>bSize;
        while(edgeCount < this->edgeNum){
            int count = 0;
            int src, des;
            while(count < this->mergeSortMaxEdge){
                if(fread(&src, sizeof(int), 1, fp_newEdgeBackup) == 0)break;
                fread(&des, sizeof(int), 1, fp_newEdgeBackup);
                blockEdgeSorted.insert(pair<int,int>(src, des));
                count++;
                edgeCount++;
            }
            string tempSortFile = this->tempSortFilePrefix + to_string(bNum) + ".dat";
            FILE * fp = fopen(tempSortFile.c_str() ,"wb");
            ftruncate(fileno(fp), this->fileSize);//resize bucketFile size 
            if(fp != NULL)
            {
                for(multimap<int,int>::iterator iter = blockEdgeSorted.begin(); iter != blockEdgeSorted.end(); iter++){
                    fwrite(&iter->first, sizeof(int), 1, fp);
                    fwrite(&iter->second, sizeof(int), 1, fp);
                }
                bSize.push_back(count);
                fclose(fp);
            }
            blockEdgeSorted.clear();
            bNum++;
        }
        fclose(fp_newEdgeBackup);
        FILE * fp_newEdge = fopen(newEdgeFile.c_str(), "wb");
        if(fp_newEdge == NULL){
            cerr << "fp_newEdge open error" << endl;
            abort();
        }
        ftruncate(fileno(fp_newEdge), this->fileSize);//resize newfile size
        FILE ** bFileList = new FILE * [bSize.size()];
        vector<Edge>buckerCurrentTopEdge;
        for(int i = 0; i < bSize.size(); i++){
            string path = tempSortFilePrefix + to_string(i) + ".dat";
            bFileList[i] = fopen(path.c_str(), "rb");
            if(bFileList[i] == NULL){
                cerr << "mergeSort open file step : error open index: " << i << endl;
            }
            int src, des;
            fread(&src, sizeof(int), 1, bFileList[i]);
            fread(&des, sizeof(int), 1, bFileList[i]);
            Edge temp;
            temp.set(src, des);
            buckerCurrentTopEdge.push_back(temp);
        }
        //buckerCurrentTopEdge set ok
        ofstream fp_txt("/media/data/demoTC/newGraphRe/newEdge.txt", ios::out);
        for(int i = 0; i < edgeNum; i++){
            int src = INT_MAX;
            int blockIndex = -1;
            for(int k = 0; k < bSize.size(); k++){
                if(buckerCurrentTopEdge[k].s < src && buckerCurrentTopEdge[k].s != -1){
                    src = buckerCurrentTopEdge[k].s;
                    blockIndex = k;
                }
            }
            int des;
            bSize[blockIndex]--;
            fwrite(&buckerCurrentTopEdge[blockIndex].s, sizeof(int), 1, fp_newEdge);
            fwrite(&buckerCurrentTopEdge[blockIndex].d, sizeof(int), 1, fp_newEdge);
            if(buckerCurrentTopEdge[blockIndex].s < buckerCurrentTopEdge[blockIndex].d){
                this->bbf.setBitSet(buckerCurrentTopEdge[blockIndex].s, buckerCurrentTopEdge[blockIndex].d);//set the bbf
            }
            fp_txt << buckerCurrentTopEdge[blockIndex].s << " " << buckerCurrentTopEdge[blockIndex].d << endl;
            if(bSize[blockIndex] != 0){
                int newSrc, newDes;
                fread(&newSrc, sizeof(int), 1, bFileList[blockIndex]);
                fread(&newDes, sizeof(int), 1, bFileList[blockIndex]);
                Edge temp;
                temp.set(newSrc, newDes);
                buckerCurrentTopEdge[blockIndex] = temp;
            }
            else{
                Edge temp;
                temp.set(-1, -1);
                buckerCurrentTopEdge[blockIndex] = temp;
                fclose(bFileList[blockIndex]);
                bFileList[blockIndex] = NULL;
            }
        }
        //cout << "before close files" << endl;
        for(int i = 0; i < bSize.size(); i++){
            if (bFileList[i] != NULL) {
                fclose(bFileList[i]);
            }
        }
        fclose(fp_newEdge);
        fp_txt.close();
        delete [] bFileList;
    }
    void IDMapping() {
        int maxOldID = getOldID2Degree();
        idCover2NewEdgeFile(maxOldID);
        mergeSort();
	}

	void buildPartitionGap() {
        int degree = 0;
        int begin = 0;
        int index;
        for(index = 1; index <= maxVertexID; index++){
            degree += index2Degree[index];
            if(degree > partitionSize){
                partitionGap.push_back(begin);
                degree = index2Degree[index];
                begin = index;
            }
        }
        if(partitionGap[partitionGap.size() - 1] != maxVertexID){
            partitionGap.push_back(maxVertexID);
        }
        cout << "cout paritionGap" << endl;
        for(auto iter : partitionGap){
            cout << iter << " ";
        }
	}

	void buildPartitionFile() {
        this->srcInPartitionList.resize(this->maxVertexID + 1);
        //cout << "PartitionFile build" << endl;
		FILE** fp_partList = new FILE*[partitionGap.size()];
		for (int i = 0; i < partitionGap.size(); i ++) {
            string path = partitionDir + to_string(i) + ".dat";
            fp_partList[i] = fopen(path.c_str(),"ab");
		}
		// sequentially read new edge file, (s, d)  d find partitionID by getPartitionIn, fp_partList[partID]==write;
				// IDasSrcInPartitionList
        //cout << "build Partition class" << endl;
        FILE * fp_in = fopen(newEdgeFile.c_str(), "rb");
        for(int i = 0; i < this->partitionGap.size(); i ++){
            int end = 0;
            if(i != this->partitionGap.size() -1) end = this->partitionGap[i + 1] -1;
            else end =  this->maxVertexID;
            Partition partition(i ,this->partitionGap[i] ,end);//this->partitionGap[i] means the partition begin
            CompanionFile cfile;
            cfile.filename = this->companionFilePath + to_string(i) + ".dat";
            cfile.fileSize = this->fileSize;
            cfile.compaionItemNum = 0;
            cfile.partitionID = i;
            partition.cfile = cfile;
            partitionList.push_back(partition);
            cfNum.push_back(0);
        }
       
        //cout << "build Partition File" << endl;
        for(int i = 0; i < this->edgeNum; i++){
            //if(i % 100000 == 0)cout << i << endl;
            int src, des;
            fread(&src , sizeof(int), 1, fp_in);
            fread(&des , sizeof(int), 1, fp_in);
            int partitionIndex = getPartitionIn(des);
            string test = partitionDir + to_string(partitionIndex) + ".txt";
            ofstream fp_txt(test, ios::out|ios::app);
            //ofstream fp_test(test,ios::app);
            //cout << "set bitset src: " << src << " partitionIndex: " << partitionIndex << endl; 
            bitsetAddIDasSrcInPartition(src, partitionIndex);
            fwrite(&src, sizeof(int), 1, fp_partList[partitionIndex]);
            fwrite(&des, sizeof(int), 1, fp_partList[partitionIndex]);
            fp_txt << src << " " << des << endl;
            fp_txt.close();
        }
        for (int i = 0; i < partitionGap.size(); i ++) {
			fclose(fp_partList[i]);
		}
        fclose(fp_in);
		//buildPartition;
        // for(int i = 1; i < this->srcInPartitionList.size(); i++){
        //     cout << "index: " << i << " asSrc: ";
        //     for(int J_asSrcPartitionIndex = 0; J_asSrcPartitionIndex <= this->partitionNum + 1 ; J_asSrcPartitionIndex++){
        //         if(this->srcInPartitionList[i][J_asSrcPartitionIndex] == true){
        //             cout << J_asSrcPartitionIndex << " ";
        //         }
        //     }
        //     cout << endl;
        // }
	}

	int getPartitionIn(int _d) {
        int index;
        for(index = 0; index < partitionGap.size(); index++){
            if(partitionGap[index] > _d){
                return index - 1;
            }
        }
        return index - 1;
	}

    void bitsetAddIDasSrcInPartition(int j,int partID){
        srcInPartitionList[j].set(partID, 1);
    }
};

int main(){
    vector<string>dataName = {"data", "wiki-shuf", "orku-shuf", "uk-2015-shuf"};
    vector<int>edgeNum = {38, 50888414, 234370166, 1566054020};
    vector<int>maxVertexID = {10, 1791489, 3072441, 39454463};
    vector<int>mergeSortMaxEdge = {2, 1310720, 2621440, 19660800};
    vector<int>partitionNum = {4, 7, 7, 7};
    for(int i = 1; i < 2; i++){
        string cmd = "../clear.sh";
        system(cmd.c_str());
        Dataset dataset ;
        dataset.edgeNum = edgeNum[i];
        dataset.fileSize = dataset.edgeNum * 8;
        //22190580
        dataset.maxVertexID = maxVertexID[i];
        dataset.partitionNum = partitionNum[i];
        dataset.partitionSize = ceil(dataset.edgeNum / dataset.partitionNum);
        //cout << "partitionSize : " << dataset.partitionSize << endl;
        clock_t start, preprocess, buildCFile, tc,finish;
        dataset.mergeSortMaxEdge = mergeSortMaxEdge[i];
        dataset.triangle = 0;
        dataset.companionFilePath = "/media/data/demoTC/compangFileRe/";
        dataset.newEdgeFile = "/media/data/demoTC/newGraphRe/newEdge.dat";
        dataset.newEdgeFileBackup = "/media/data/demoTC/newGraphRe/newEdgeBackup.dat";
        dataset.partitionDir = "/media/data/demoTC/PartitionFileRe/";
        dataset.dataPath = "/media/data/demoTC/dataSet/" + dataName[i] + ".txt";
        dataset.tempSortFilePrefix = "/media/data/demoTC/SortFile/";
        string timeRecord = "/media/data/demoTC/timeRecord/" + dataName[i] + "re.txt";
        unsigned long long int _bitSize = ceil(dataset.edgeNum*6.4 - dataset.maxVertexID * (dataset.partitionNum +1 ));
        int _hashNum = ceil(_bitSize / dataset.edgeNum * 0.693147);
        dataset.bbf.setBBF(_bitSize, _hashNum);//unsigned long long int _bitSetSize, int _hashNum
        ofstream fp(timeRecord, ios::out);
        start = clock();
        dataset.preprocess();
        preprocess = clock();
        dataset.buildCompanionFile();
        buildCFile = clock();
        dataset.triangleCount();
        tc = clock();
        dataset.writeCompanionFileNum(fp);
        fp << "preprocess use time :" << (double)(preprocess - start)/CLOCKS_PER_SEC << " s" << endl;
        fp << "buildCompanionFile time :" << (double)(buildCFile - preprocess)/CLOCKS_PER_SEC << " s" << endl;
        fp << "tc time :" << (double)(tc - buildCFile)/CLOCKS_PER_SEC << " s" << endl;
        fp << "total time: " << (double)(tc - start)/CLOCKS_PER_SEC << " s" << endl;
        fp << "triangele count: " << dataset.triangle<<endl;
        fp.close();
        malloc_trim(0);
        //dataset.bbf.showBitSet();
    }
    return 0;
}