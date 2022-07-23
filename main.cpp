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
	unordered_map<int, int> id2Degree;
    vector<int>index2Degree;
	vector<int>partitionGap;
	map<int, vector<int> > IDasSrcInPartitionList;
	vector<Partition> partitionList;
    static bool cmp(idDegree  a, idDegree  b){
        if(a.degree < b.degree){
            return true;
        }
        else if (a.degree == b.degree){
            return a.id < b.id;
        }
        else return false;
    }
    vector<string> split(const string& str, const string& delim) {
        vector<string> res;
        if("" == str) return res;
        char * strs = new char[str.length() + 1] ; 
        strcpy(strs, str.c_str()); 
    
        char * d = new char[delim.length() + 1];
        strcpy(d, delim.c_str());
    
        char *p = strtok(strs, d);
        while(p) {
            string s = p; 
            res.push_back(s); 
            p = strtok(NULL, d);
        }
        return res;
    }
    int getSrcAndDes(string data, int index){
        vector<string>splitVec = split(data, " ");
        return std::stoi(splitVec[index]);
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

    bool buildEncodeAndPersist(string edgeFile, string outputEncodeFile, int kDim) {

    }

    bool loadEncode() {

    }

    bool batchVendTest(int j, vector<int> K) {

    }

	bool buildCompanionFile(){
		// open companion file in write mode
		//sequentially edgeTable on disk;
        FILE * fp = fopen(this->newEdgeFile.c_str(), "rb");
        int src,des;
        fread(&src, sizeof(int), 1, fp);
        fread(&des, sizeof(int), 1, fp);
        int edgeCount = 1;
        for(int i = 1; i <= maxVertexID; i++){
            if(i % 100000 == 0)cout << "write companion file index: " << i << endl;
            vector<int>i_adj;
            while(i == src && edgeCount <= edgeNum){
                i_adj.push_back(des);
                if(fread(&src, sizeof(int), 1, fp) == 0) break;
                fread(&des, sizeof(int), 1, fp);
                edgeCount++;
            }//获取i的邻接表
            // sort(i_adj.begin(), i_adj.end());
            // i_adj.erase(unique(i_adj.begin(), i_adj.end()), i_adj.end());
            //邻接表去重
            for(int adj_index = 0; adj_index < i_adj.size(); adj_index++){
                int j = i_adj[adj_index];//从i的邻接表出发
                //cout << "i :" << i << " j: " << j << endl
                if(j > i){// j 大于 i
                    vector<int>J_asSrcInPartitionList = this->IDasSrcInPartitionList[j];
                    for(int partitionI = 0; partitionI < J_asSrcInPartitionList.size(); partitionI++){//从j所在的Partition分区出发
                        int J_asSrcPartitionIndex = J_asSrcInPartitionList[partitionI];//获取分区index
                        Partition partition = this->partitionList[J_asSrcPartitionIndex];//获取分区Partition
                        // cout << "i_adj" << endl;
                        // for(auto i :i_adj){
                        //     cout << i << " ";
                        // }
                        // cout << endl;
                        // cout << "partition begin : " << partition.begin << " partition end: " << partition.end << endl;
                        vector<int>i_adj_copy;
                        for(int i = 0; i< i_adj.size(); i++){
                            if(i_adj[i] > j && partition.begin <= i_adj[i]&& i_adj[i] <= partition.end ){//第三点大于j 且在区间内
                                i_adj_copy.push_back(i_adj[i]);
                                //cout << "success " << i_adj[i] << " ";
                            }
                        }
                        //cout << endl;
                        //vend 此处j 和 i_adj_copy 做一个batchVendTest
                        //将i中符合Partition begin 和 end 的元素加入到 i_adj_copy
                        string test = this->companionFilePath + to_string(J_asSrcPartitionIndex) +".txt";
                        ofstream fp_test(test,ios::app);
                        if(i_adj_copy.size() != 0){//若不为0
                            //cout << "write companin file" << endl;
                            CFTriple cftriple;
                            cftriple.i = i;
                            cftriple.j = j;
                            cftriple.neighborList = i_adj_copy;
                            fp_test << i << " " << j << " " << i_adj_copy.size() << " ";
                            for(auto i : i_adj_copy){
                                fp_test << i << " ";
                            }
                            fp_test << endl;
                            partition.cfile.Open(WRITE,true);
                            partition.cfile.Write(cftriple);//写入cfile
                            partition.cfile.Close();
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
            //cout << "triangleCount compaion file index: " << partitionI << endl;
            
            //read all vertex's adj in this paritition
            string path = partitionDir + to_string(partitionI) + ".dat";
            //cout << "read all adj in partitionName " << partitionI << endl;
            if(this->partitionList[partitionI].cfile.Open(READ,false)==true){
                FILE * fp = fopen(path.c_str(),"rb");
                map<int, vector<int>> partitionAdj = this->partitionList[partitionI].LoadAdj(fp);
                // for(auto test = partitionAdj.begin(); test != partitionAdj.end(); test ++){
                //     cout << "j: " << test->first << endl;
                //     for(auto test_1 : test->second){
                //         cout << test_1 << " ";
                //     }
                //     cout << endl;
                // }
                fclose(fp); 
                int test_count = 0;
                if(this->partitionList[partitionI].cfile.fp != NULL){//该partition file 是否被创建
                    int i, j, size;
                    while(this->partitionList[partitionI].cfile.hasNext()){//如果有下一个cftripe
                        //if(test_count % 10000 == 0)cout <<  "triangleCount compaion file partitionI: " << partitionI << "read cftriple "<< test_count <<endl;
                        CFTriple cftriple = this->partitionList[partitionI].cfile.readNext();//读取cftripe
                        if(cftriple.i == -1)break;
                        vector<int>thisPartitionAdjOfJ = partitionAdj[cftriple.j];
                        vector<int>intersec;
                        // cout << "cftriple.neighborList" << endl;
                        // for(auto i : cftriple.neighborList){
                        //     cout << i << " ";
                        // }
                        // cout << endl;
                        // cout << "thisPartitionAdjOfJ j: " << cftriple.j << endl;
                        // for(auto i :thisPartitionAdjOfJ){
                        //     cout << i << " ";
                        // }
                        // cout << endl;
                        std::set_intersection(cftriple.neighborList.begin(), cftriple.neighborList.end(),
                                    thisPartitionAdjOfJ.begin(), thisPartitionAdjOfJ.end(),
                                    std::back_inserter(intersec));//交集
                        this->triangle += intersec.size();
                        test_count++;
                    }
                }
                partitionList[partitionI].cfile.Close();
            }
            //cout << "companion file index: " << partitionI << " triangle num " << this->triangle << endl;
        }
        return triangle;
	}

private:
	void IDMapping() {
        
        cout << "begin IDMapping" << endl;
		//build prevId2prevDegree;
        ifstream fp(this->dataPath, ios::in);
        string temp;
        int maxOldID = 0;
        while(getline(fp,temp)){
            if(temp.at(0) > '9' || temp.at(0) < '0' || temp.length() < 1) {
                break;
            }
            stringstream _ss;
            int src, des;
            _ss << temp;
            _ss >> src >> des;
            maxOldID = (maxOldID < src) ? src : maxOldID;
            maxOldID = (maxOldID < des) ? des : maxOldID;
            if(src < des){
                this->id2Degree[src] ++;
                this->id2Degree[des] ++;
            }
            
        }
        fp.close();

		//revere prevDegree2prevID;
        //sort all the ID by degree
        cout << "begin sort" << endl;
        vector<idDegree>sortVector;
        //sortVector.resize(maxOldID);
        int i = 0;
        for (unordered_map<int,int>::iterator it = id2Degree.begin(); it!=id2Degree.end(); ++it)
        {
           idDegree temp;
           temp.degree = it->second;
           temp.id = it->first;
           //sortVector[i] = temp;
           sortVector.push_back(temp);
           i++;
        }
        sort(sortVector.begin(), sortVector.end(), cmp);
        cout << "sort end" << endl;
		// newDegree2NewID; (i.e., id2Degree)
		// prevID2newID;

        // this->index2Degree.clear();
        // int* pos2newID = new int[maxOldID+1];
        // //ofstream fp_oldID2newID(this->OldID2NewIDFile, ios::out);
        // int idCurrent = 1;//newID begin from 1
        // cout << "begin pos@newID" << endl; 
        // for (auto iter = sortVector.begin(); iter != sortVector.end(); iter ++) {
        //     pos2newID[iter->id] = idCurrent;
        //     this->index2Degree.push_back(iter->degree);
        //     idCurrent ++;
        // }
        // cout << "pos@newID succsee " << endl;

        this->index2Degree.clear();
        int* pos2newID = new int[maxOldID+1];
        //ofstream fp_oldID2newID(this->OldID2NewIDFile, ios::out);
        int idCurrent = 1;//newID begin from 1
        for (auto iter = sortVector.begin(); iter != sortVector.end(); iter ++) {
            pos2newID[iter->id] = idCurrent;
            this->index2Degree.push_back(iter->degree);
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
        
        // int _buf_size = edgeNum / 10;
        // for(int i = 0; i < 10; i++){
        //     int * _edge_buffer = new int[_buf_size];
        //     int * _new_edge = new int[_buf_size];
        //     int _read_count = loadBatch(fp_oriEdgeFile, _edge_buffer, _buf_size);
        //     #pragma omp parallel for num_threads(40)
        //     for(int i = 0; i < _read_count; i ++) {
        //         _new_edge[i*2] = pos2newID[_edge_buffer[i*2]];
        //         _new_edge[i*2+1] = pos2newID[_edge_buffer[i*2+1]];
        //         //cout << "old " << _edge_buffer[i*2] << "->" << _edge_buffer[i*2+1] << endl;
        //         //cout << "new " << _new_edge[i*2] << "->" << _new_edge[i*2+1] << endl;
        //     }
        //     fwrite(_new_edge, sizeof(int), _read_count, fp_NewIDEdgeFileBackup);
        // }
    
        string _buf;
        int * edge = new int [this->edgeNum * 2];
        int k = 0;
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
            edge[k * 2] = newS;
            edge[k * 2 + 1] =newD;
            k++;
        }
        fwrite(edge, sizeof(int), this->edgeNum * 2, fp_NewIDEdgeFileBackup);

        fclose(fp_NewIDEdgeFileBackup);
        fp_oriEdgeFile.close();
        cout<< "new file success" << endl;
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
            
            while(count < this->mergeSortMaxEdge && edgeCount < this->edgeNum){
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
        int errNum = 0;
        FILE * fp_newEdge = NULL;
        if((fp_newEdge = fopen(newEdgeFile.c_str(), "wb"))== NULL){
            errNum = errno;
            cerr << errNum << endl;
            cerr << "fp_newEdge open " << endl;
            abort();
        }
        ftruncate(fileno(fp_newEdge), this->fileSize);//resize newfile size
        FILE * bFileList [bSize.size()] ={nullptr};
        int minSrcInPartition[bSize.size()] ={0};

        for(int i = 0; i < bSize.size(); i++){
            string path = tempSortFilePrefix + to_string(i) + ".dat";
            bFileList[i] = fopen(path.c_str(), "rb");
            if(bFileList[i] == NULL){
                cerr << "mergeSort open file step : error open index: " << i << endl;
            }
            int src;
            fread(&src, sizeof(int), 1, bFileList[i]);
            minSrcInPartition[i] = src;
        }
        //minSrcInPartition set ok
        for(int i = 0; i < edgeNum; i++){
            int src = INT_MAX;
            int blockIndex = -1;
            for(int k = 0; k < bSize.size(); k++){
                if(minSrcInPartition[k] < src && minSrcInPartition[k] != -1){
                    src = minSrcInPartition[k];
                    blockIndex = k;
                }
            }
            int des;
            fread(&des, sizeof(int), 1, bFileList[blockIndex]);
            bSize[blockIndex]--;
            fwrite(&src, sizeof(int), 1, fp_newEdge);
            fwrite(&des, sizeof(int), 1, fp_newEdge);
            if(bSize[blockIndex] != 0){
                int newBlockFlag = 0;
                fread(&newBlockFlag, sizeof(int), 1, bFileList[blockIndex]);
                minSrcInPartition[blockIndex] = newBlockFlag;
            }
            else{
                minSrcInPartition[blockIndex] = -1;
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
	}

	void buildPartitionFile() {
        //cout << "PartitionFile build" << endl;
		FILE** fp_partList = new FILE*[partitionGap.size()];
		for (int i = 0; i < partitionGap.size(); i ++) {
			FILE* fp = fp_partList[i];
            string path = partitionDir + to_string(i) + ".dat";
			fp = fopen(path.c_str(),"ab");
            fp_partList[i] = fp;
            ftruncate(fileno(fp_partList[i]), this->fileSize);
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
        }
       
        //cout << "build Partition File" << endl;
        for(int i = 0; i < this->edgeNum; i++){
            //if(i % 100000 == 0)cout << i << endl;
            int src, des;
            fread(&src , sizeof(int), 1, fp_in);
            fread(&des , sizeof(int), 1, fp_in);
            int partitionIndex = getPartitionIn(des);
            string test = partitionDir + to_string(partitionIndex) + ".txt";
            //ofstream fp_test(test,ios::app);
            addIDasSrcInPartition(src, partitionIndex);
            fwrite(&src, sizeof(int), 1, fp_partList[partitionIndex]);
            fwrite(&des, sizeof(int), 1, fp_partList[partitionIndex]);
            //fp_test << src << " " << des << endl;
            partitionList[partitionIndex].edgeNum ++ ;
        }
        for (int i = 0; i < partitionGap.size(); i ++) {
			fclose(fp_partList[i]);
		}
        fclose(fp_in);
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

	void addIDasSrcInPartition(int j, int partID) {
        map<int, vector<int>>::iterator iter =  IDasSrcInPartitionList.find(j);
        vector<int>list;
        if(iter != IDasSrcInPartitionList.end()) list = IDasSrcInPartitionList[j];
        list.push_back(partID);
        sort(list.begin(), list.end());
        list.erase(unique(list.begin(), list.end()), list.end());
        IDasSrcInPartitionList[j] = list;
	}
};

int main(){
#if _OPENMP
        cout << " support openmp " << endl;
#else
        cout << " not support openmp" << endl;
#endif

    vector<string>dataName;
    dataName.push_back("data");
    dataName.push_back("as-shuf");
    dataName.push_back("wiki-shuf");
    dataName.push_back("orku-shuf");
    dataName.push_back("ar-shuf");
    dataName.push_back("uk-2015-shuf");
    vector<int>edgeNum = {38,22190596, 50888414, 234370166, 1107805994, 1566054020};
    vector<int>maxVertexID = {10,1696415, 1791489, 3072441, 22743881, 29452263};
    vector<int>mergeSortMaxEdge = {2,1310720, 1310720, 2621440, 11796480, 19660800};
    for(int i = 1; i < 2; i++){
        Dataset dataset ;
        dataset.edgeNum = edgeNum[i];
        dataset.fileSize = dataset.edgeNum * 8;
        //22190580
        dataset.maxVertexID = maxVertexID[i];
        dataset.partitionNum = 5;
        dataset.partitionSize = ceil(dataset.edgeNum / dataset.partitionNum);
        //cout << "partitionSize : " << dataset.partitionSize << endl;
        clock_t start, preprocess, buildCFile, tc,finish;
        dataset.mergeSortMaxEdge = mergeSortMaxEdge[i];
        dataset.triangle = 0;
        dataset.companionFilePath = "/media/data/vend/triangleCount/companyFile/";
        dataset.newEdgeFile = "/media/data/vend/triangleCount/doubleEdgeProject/newGraph/newEdge.dat";
        dataset.newEdgeFileBackup = "/media/data/vend/triangleCount/doubleEdgeProject/newGraph/newEdgeBackup.dat";
        dataset.partitionDir = "/media/data/vend/triangleCount/PartitionFile/";
        dataset.dataPath = "/media/data/vend/triangleCount/doubleEdge/" + dataName[i] + ".txt";
        dataset.tempSortFilePrefix = "/media/data/vend/triangleCount/SortFile/";
        dataset.tempFile = "/media/data/vend/triangleCount/tempFile/";
        dataset.OldID2NewIDFile = "/media/data/vend/triangleCount/backupFile/oldID2newID";
        string timeRecord = "/media/data/vend/triangleCount/timeRecord/" + dataName[i] + ".txt";
        ofstream fp(timeRecord, ios::out);
        start = clock();
        dataset.preprocess();
        preprocess = clock();
        dataset.buildCompanionFile();
        buildCFile = clock();
        dataset.triangleCount();
        cout<<"finsh" << endl;
        tc = clock();
        fp << "use time :" << (double)(tc - buildCFile)/CLOCKS_PER_SEC << " s" << endl;
        fp << "tc finish()" <<endl;
        fp << "preprocess use time :" << (double)(preprocess - start)/CLOCKS_PER_SEC << " s" << endl;
        fp << "buildCompanionFile time :" << (double)(buildCFile - preprocess)/CLOCKS_PER_SEC << " s" << endl;
        fp << "tc time :" << (double)(tc - buildCFile)/CLOCKS_PER_SEC << " s" << endl;
        fp << "total time: " << (double)(tc - start)/CLOCKS_PER_SEC << " s" << endl;
        fp << "triangele count: " << dataset.triangle<<endl;
        fp.close();
        string cmd = "./clear.sh";
        //system(cmd.c_str());
    }
    return 0;
}