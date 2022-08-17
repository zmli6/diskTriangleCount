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
#include "logging.h"
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
	vector<int> J_list;
	vector<int> I_neighborList;
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

	inline CFTriple readNext() {
        CFTriple temp;
        int i, j;
        if(fread(&i, sizeof(int), 1,fp)==0){
            temp.i = -1;
            return temp;
        }
        temp.i = i;
        int size;
        fread(&size, sizeof(int), 1,fp);
        temp.J_list.resize(size);
        fread(&temp.J_list[0], sizeof(int)*size, 1,fp);
        fread(&size, sizeof(int), 1,fp);
        temp.I_neighborList.resize(size);
        fread(&temp.I_neighborList[0], sizeof(int)*size, 1, fp);
        return temp;
	}

	inline bool Write(CFTriple& cfTriple) {
        int i = cfTriple.i;
        fwrite(&i, sizeof(int), 1,fp);
        int size = cfTriple.J_list.size();
        fwrite(&size, sizeof(int), 1,fp);
        fwrite(&cfTriple.J_list[0], sizeof(int)*size, 1, fp);
        size = cfTriple.I_neighborList.size();
        fwrite(&size, sizeof(int), 1,fp);
        fwrite(&cfTriple.I_neighborList[0], sizeof(int)*size, 1, fp);
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

	inline map<int, vector<int>> LoadAdj(FILE * fp) {
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
    int newEdgeNum;
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
    string bitsetRecord;
    int * id2Degree;
    vector<int>index2Degree;
	vector<int>partitionGap;
	vector<Partition> partitionList;
    vector<unsigned long long int>cfNum;
    vector<bitset<8>>srcInPartitionList;
    BBF bbf;
    logging log;
    static bool cmp(idDegree  a, idDegree  b){
        if(a.degree < b.degree){
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
            for(int partitionID = 0; partitionID < this->partitionList.size(); partitionID ++){
                Partition partition = this->partitionList[partitionID];
                vector<int>jInPartition;//在当前partition的J
                vector<int> i_adj2CompangFile;//companyFile中i的邻居部分
                for(int adj_index = 0; adj_index < i_adj.size(); adj_index++){
                    int j = i_adj[adj_index];
                    bitset<8>J_asSrcInPartitionList = srcInPartitionList[j];    
                    if(J_asSrcInPartitionList[partitionID] == true){
                        jInPartition.push_back(j);
                    }
                    if(j >= partition.begin && j <= partition.end){
                        i_adj2CompangFile.push_back(j);
                    }
                }//获得属于当前partition的j
                vector<int>j2CompanyFile;
                if(i_adj2CompangFile.size() != 0){
                    for(int index = 0; index < jInPartition.size(); index++){
                        if(this->bbf.NEpairTest(jInPartition[index], i_adj2CompangFile) == false){
                            j2CompanyFile.push_back(jInPartition[index]);
                        }
                    }
                    if(j2CompanyFile.size() != 0){
                        CFTriple cftriple;
                        cftriple.i = i;
                        cftriple.J_list = j2CompanyFile;
                        cftriple.I_neighborList = i_adj2CompangFile;
                        // fp << i << " " << j << " " << i_adj_copy.size();
                        // for(auto item : i_adj_copy){
                        //     fp << " " << item;
                        // }
                        // fp << endl;
                        partition.cfile.Open(WRITE,true);
                        partition.cfile.Write(cftriple);//写入cfile
                        cfNum[partitionID]++;
                        partition.cfile.Close();
                    }
                }               
            }
        }
        fclose(fp);
        cout << "finsh companion file" << endl;
        return true;
	}
    inline int vecIntersectionBinary(vector<int>biggerVec, vector<int>smallerVec){
        int count = 0;
        for(int i = 0; i < smallerVec.size(); i++){
            int left = 0;int right = biggerVec.size() - 1;
            while (left < right) {
                int mid = left + (right - left) / 2;
                if (smallerVec[i] > biggerVec[mid]) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            if (biggerVec[left] == smallerVec[i]){
                count++;
            }
        }
        return count;
    }
    inline int vecIntersection(vector<int>JNeighbor, vector<int>INeighbor){
        // int size1 = JNeighbor.size();
        // int size2 = INeighbor.size();
        // if(size1 > size2){
        //     swap(size1, size2);
        // }
        // if(size2 / size1 >= 32){
        //     if(JNeighbor.size() > INeighbor.size()){
        //         return vecIntersectionBinary(JNeighbor, INeighbor);
        //     }
        //     else{
        //         return vecIntersectionBinary(INeighbor, JNeighbor);
        //     }
        // }
        // else{
        //     vector<int>intersec;
        //     std::set_intersection(JNeighbor.begin(), JNeighbor.end(),
        //                         INeighbor.begin(), INeighbor.end(),
        //                         std::back_inserter(intersec));//交集
        //     return intersec.size();
        // }
        vector<int>intersec;
        std::set_intersection(JNeighbor.begin(), JNeighbor.end(),
                            INeighbor.begin(), INeighbor.end(),
                            std::back_inserter(intersec));//交集
        return intersec.size();
    }

	int triangleCount() {
		for(int partitionI = 0; partitionI < this->partitionList.size(); partitionI++){
            //read all vertex's adj in this paritition
            string path = partitionDir + to_string(partitionI) + ".dat";
            if(this->partitionList[partitionI].cfile.Open(READ,false)==true){
                FILE * fp = fopen(path.c_str(),"rb");
                map<int, vector<int>> partitionAdj = this->partitionList[partitionI].LoadAdj(fp);
                for(auto & item : partitionAdj){
                    sort(item.second.begin(), item.second.end());
                }
                fclose(fp); 
                int test_count = 0;
                if(this->partitionList[partitionI].cfile.fp != NULL){//该partition file 是否被创建
                    int i, j, size;
                    while(this->partitionList[partitionI].cfile.hasNext()){//如果有下一个cftripe
                        if(test_count % 10000 == 0)cout <<  "triangleCount compaion file partitionI: " << partitionI << "read cftriple "<< test_count <<endl;
                        CFTriple cftriple = this->partitionList[partitionI].cfile.readNext();//读取cftripe
                        sort(cftriple.I_neighborList.begin(),cftriple.I_neighborList.end());
                        if(cftriple.i == -1)break;
                        for(int i = 0; i < cftriple.J_list.size(); i++){
                            int j = cftriple.J_list[i];
                            vector<int>thisPartitionAdjOfJ = partitionAdj[j];
                            vector<int>intersec;
                            std::set_intersection(thisPartitionAdjOfJ.begin(), thisPartitionAdjOfJ.end(),
                                                cftriple.I_neighborList.begin(), cftriple.I_neighborList.end(),
                                                std::back_inserter(intersec));//交集
                            this->triangle += intersec.size();
                            test_count++;
                        }
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

    void RecordCF(){
        for(int i = 0; i < this->partitionList.size(); i++){
            string path = companionFilePath + to_string(i) + ".dat";
            if(this->partitionList[i].cfile.Open(READ,false)==true){
                this->partitionList[i].cfile.Close();
                this->log.writeParition(path, i, 1000);
            }
            cout << "write Partition " << i << endl;
        }
        this->log.writeParition(10000);
    }
private:
    int getOldID2Degree(){
        cout << "begin IDMapping" << endl;
		//build prevId2prevDegree;
        ifstream fp(this->dataPath, ios::in);
        if(!fp){
            cout << "file open error " << endl;
        }
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
        int* pos2newID = new int[_maxOldID+1];
        int idCurrent = 1;//newID begin from 1
        this->index2Degree.push_back(0);//ID index begin from 1
        for (auto iter = sortVector.begin(); iter != sortVector.end(); iter ++) {
            pos2newID[iter->id] = idCurrent;
            this->index2Degree.push_back(iter->degree);
            idCurrent ++;
        }
        if(this->log.getLog() == true){
            this->log.degreeSave(this->index2Degree);
        }
		// sequentially read prev edge file and output new edge into new edgefile
        ifstream fp_oriEdgeFile(this->dataPath, ios::in);
        FILE * fp_NewIDEdgeFileBackup = fopen(this->newEdgeFileBackup.c_str(), "wb");
        //ftruncate(fileno(fp_NewIDEdgeFileBackup), this->fileSize);
        if(fp_NewIDEdgeFileBackup == NULL){
            cout << "error open fp_NewIDEdgeFileBackup" << endl;
            abort();
        }
        string _buf;
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
            if(newS < newD){
                fwrite(&newS, sizeof(int), 1, fp_NewIDEdgeFileBackup);
                fwrite(&newD, sizeof(int), 1, fp_NewIDEdgeFileBackup);
                this->newEdgeNum++;
            }
            
        }
        fclose(fp_NewIDEdgeFileBackup);
        fp_oriEdgeFile.close();
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
        while(edgeCount < this->newEdgeNum){
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
            //ftruncate(fileno(fp), this->fileSize);//resize bucketFile size 
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
        //ftruncate(fileno(fp_newEdge), this->fileSize);//resize newfile size
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
        for(int i = 0; i < this->newEdgeNum; i++){
            if(i % 100000 == 0){
                cout <<"merge Sort is " << i << endl;
            }
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
                //cout << "set bitsSet " << buckerCurrentTopEdge[blockIndex].s << " " << buckerCurrentTopEdge[blockIndex].d << endl;
            this->bbf.setBitSet(buckerCurrentTopEdge[blockIndex].s, buckerCurrentTopEdge[blockIndex].d);
            
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
        delete [] bFileList;
    }
    void IDMapping() {
        int maxOldID = getOldID2Degree();
        idCover2NewEdgeFile(maxOldID);
        mergeSort();
        if(this->log.getLog() == true){
            cout << "bitSet record" << endl;
            string FilePath = this->bitsetRecord + "bitset.txt";
            string DataPath = this->bitsetRecord + "bitset_data.txt";
            this->bbf.saveBitSet(FilePath, DataPath);
        }
	}

	void buildPartitionGap() {
        int degree = 0;
        int begin = 1;
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
        if(this->log.getLog() == true){
            this->log.degreeGapSave(partitionGap);
        }
	}

	void buildPartitionFile() {
        this->srcInPartitionList.resize(this->maxVertexID + 1);
        //cout << "PartitionFile build" << endl;
		FILE** fp_partList = new FILE*[partitionGap.size()];
		for (int i = 0; i < partitionGap.size(); i ++) {
            string path = partitionDir + to_string(i) + ".dat";
            fp_partList[i] = fopen(path.c_str(),"ab");
            //ftruncate(fileno(fp_partList[i]), this->fileSize);
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
            cfile.partitionID = i;
            partition.cfile = cfile;
            partitionList.push_back(partition);
            cfNum.push_back(0);
        }
       
        //cout << "build Partition File" << endl;
        for(int i = 0; i < this->newEdgeNum; i++){
            //if(i % 100000 == 0)cout << i << endl;
            int src, des;
            fread(&src , sizeof(int), 1, fp_in);
            fread(&des , sizeof(int), 1, fp_in);
            int partitionIndex = getPartitionIn(des);
            //string test = partitionDir + to_string(partitionIndex) + ".txt";
            //ofstream fp_test(test,ios::app);
            //cout << "set bitset src: " << src << " partitionIndex: " << partitionIndex << endl; 
            bitsetAddIDasSrcInPartition(src, partitionIndex);
            fwrite(&src, sizeof(int), 1, fp_partList[partitionIndex]);
            fwrite(&des, sizeof(int), 1, fp_partList[partitionIndex]);
        }
        for (int i = 0; i < partitionGap.size(); i ++) {
			fclose(fp_partList[i]);
		}
        fclose(fp_in);
        delete [] fp_partList;
		//buildPartition;
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
    for(int i = 0; i < 1; i++){
        // string cmd = "../clear.sh";
        // system(cmd.c_str());
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
        dataset.newEdgeNum = 0;
        dataset.companionFilePath = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/companyFile/";
        dataset.newEdgeFile = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/newGraph/newEdge.dat";
        dataset.newEdgeFileBackup = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/newGraph/newEdgeBackup.dat";
        dataset.partitionDir = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/PartitionFile/";
        dataset.dataPath = "/media/hnu/hnu2022/liziming/diskTC/dataSet/" + dataName[i] + ".txt";
        dataset.tempSortFilePrefix = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/SortFile/";
        string timeRecord = "/media/hnu/hnu2022/liziming/diskTC/timeRecord/" + dataName[i] + ".txt";
        dataset.bitsetRecord = "/media/hnu/hnu2022/liziming/diskTC/" + dataName[i] + "/BitSet/";
        unsigned long long int _bitSize = ceil(dataset.edgeNum*6.4 - dataset.maxVertexID * (dataset.partitionNum +1 ));
        int _hashNum = ceil(_bitSize / dataset.edgeNum * 0.693147);
        dataset.bbf.setBBF(_bitSize, _hashNum);//unsigned long long int _bitSetSize, int _hashNum
        dataset.log.setLog(true);
        dataset.log.setFilePath("/media/hnu/hnu2022/liziming/diskTC/" + dataName[i]);
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
        if(dataset.log.getLog() == true)dataset.RecordCF();
        //malloc_trim(0);
        //dataset.bbf.showBitSet();
    }
    return 0;
}