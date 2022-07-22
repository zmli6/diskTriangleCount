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
using namespace std;
enum OpenMode {
    READ,
    WRITE
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
};

class CompanionFile {
public:
	FILE * fp;
	string filename;
	int partitionID;

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
        //cout << "cfile read cftriple i:" << temp.i << " j:" << temp.j << " size:" << temp.neighborList.size() << "in Partition Index " << partitionID << endl;
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
        //cout << "cfile write cftriple i:" << i << " j:" << j << " size:" << cfTriple.neighborList.size() << "in Partition Index " << partitionID << endl;
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

	string sortedBySrcPathDat;
	string partition;
    string companionFilePath;
    string newEdgeFile;
    string newEdgeFileBackup;
    string partitionDir;
    string dataPath;
    string tempSortFilePrefix;
    string newEdgeFileTxt;
    string tempFile;
	unordered_map<int, int> id2Degree;
    vector<int>index2Degree;
    unordered_map<int, int> OldID2NewID;
	vector<int>partitionGap;
	map<int, vector<int> > IDasSrcInPartitionList;
	vector<Partition> partitionList;
        vector<string> split(const string& str, const string& delim) {
        vector<string> res;
        if("" == str) return res;
        //先将要切割的字符串从string类型转换为char*类型
        char * strs = new char[str.length() + 1] ; //不要忘了
        strcpy(strs, str.c_str()); 
    
        char * d = new char[delim.length() + 1];
        strcpy(d, delim.c_str());
    
        char *p = strtok(strs, d);
        while(p) {
            string s = p; //分割得到的字符串转换为string类型
            res.push_back(s); //存入结果数组
            p = strtok(NULL, d);
        }
        return res;
    }
    int getSrcAndDes(string data, int index){
        vector<string>splitVec = split(data, " ");//获取id1 id2
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

    bool buildEncodeAndPersist(sting edgeFile, string outputEncodeFile, int kDim) {

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
        for(int i = 0; i < maxVertexID; i++){
            if(i % 100000 == 0)cout << "write companion file index: " << i << endl;
            vector<int>i_adj;
            while(i == src && edgeCount <= edgeNum){
                i_adj.push_back(des);
                if(fread(&src, sizeof(int), 1, fp) == 0) break;
                fread(&des, sizeof(int), 1, fp);
                edgeCount++;
            }//获取i的邻接表
            sort(i_adj.begin(), i_adj.end());
            i_adj.erase(unique(i_adj.begin(), i_adj.end()), i_adj.end());
            //cout << "unique success " << endl;
            //邻接表去重
            for(int adj_index = 0; adj_index < i_adj.size(); adj_index++){
                int j = i_adj[adj_index];//从i的邻接表出发
                //cout << "i :" << i << " j: " << j << endl;
                if(j > i){// j 大于 i
                    vector<int>J_asSrcInPartitionList = this->IDasSrcInPartitionList[j];
                    for(int partitionI = 0; partitionI < J_asSrcInPartitionList.size(); partitionI++){//从j所在的Partition分区出发
                        int J_asSrcPartitionIndex = J_asSrcInPartitionList[partitionI];//获取分区index
                        Partition partition = this->partitionList[J_asSrcPartitionIndex];//获取分区Partition
                        vector<int>i_adj_copy;
                        for(int i = 0; i< i_adj.size(); i++){
                            if(i_adj[i] > j && partition.begin <= i_adj[i]&& i_adj[i] <= partition.end ){//第三点大于j 且在区间内
                                i_adj_copy.push_back(i_adj[i]);
                            }
                        }
                        //将i中符合Partition begin 和 end 的元素加入到 i_adj_copy
                        if(i_adj_copy.size() != 0){//若不为0
                            //cout << "write companin file" << endl;
                            string path = "/media/data/vend/triangleCount/companyFile/" + to_string(J_asSrcPartitionIndex) + ".txt";
                            ofstream fp(path,ios::app);
                            CFTriple cftriple;
                            cftriple.i = i;
                            cftriple.j = j;
                            cftriple.neighborList = i_adj_copy;
                            fp << i <<" " << j << " " << i_adj_copy.size() << " ";
                            for(auto item : i_adj_copy){
                                fp << item << " ";
                            }
                            fp << endl;
                            fp.close();
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
            cout << "triangleCount compaion file index: " << partitionI << endl;
            
            //read all vertex's adj in this paritition
            string path = partitionDir + to_string(partitionI) + ".dat";
            FILE * fp = fopen(path.c_str(),"rb");
            map<int, vector<int>> partitionAdj = this->partitionList[partitionI].LoadAdj(fp);
            fclose(fp);
            cout << "read all adj in partitionName " << partitionI << endl;
            if(this->partitionList[partitionI].cfile.Open(READ,false)==true){
                int test_count = 0;
                if(this->partitionList[partitionI].cfile.fp != NULL){//该partition file 是否被创建
                    int i, j, size;
                    while(this->partitionList[partitionI].cfile.hasNext()){//如果有下一个cftripe
                        if(test_count % 10000 == 0)cout <<  "triangleCount compaion file partitionI: " << partitionI << "read cftriple "<< test_count <<endl;
                        CFTriple cftriple = this->partitionList[partitionI].cfile.readNext();//读取cftripe
                        if(cftriple.i == -1)break;
                        vector<int>thisPartitionAdjOfJ = partitionAdj[cftriple.j];
                        vector<int>intersec;
                        std::set_intersection(cftriple.neighborList.begin(), cftriple.neighborList.end(),
                                    thisPartitionAdjOfJ.begin(), thisPartitionAdjOfJ.end(),
                                    std::back_inserter(intersec));//交集
                        this->triangle += intersec.size();
                        test_count++;
                    }
                }
                partitionList[partitionI].cfile.Close();
            }
            cout << "companion file index: " << partitionI << " triangle num " << this->triangle << endl;
        }
        return triangle;
	}

private:
	void IDMapping() {
        
        cout << "begin IDMapping" << endl;
		//build prevId2prevDegree;
        ifstream fp(dataPath, ios::in);
        string temp;
        while(getline(fp,temp)){
            if(temp.at(0) > '9' || temp.at(0) < '0' || temp.length() < 1) {
                break;
            }
            int src = getSrcAndDes(temp, 0);
            int des = getSrcAndDes(temp, 1);
            if(src < des){
                this->id2Degree[src] ++;
                this->id2Degree[des] ++;
            }
            
        }
        fp.close();
        // {// degree test
        //     for (auto iter = id2Degree.begin(); iter != id2Degree.end(); iter ++) {
        //         cout << iter->first << ", " << iter->second << endl;
        //     }
        // }

        
		//revere prevDegree2prevID;
        //sort all the ID by degree
        multimap<int, int > degree2PrevID;
        for (unordered_map<int,int>::iterator it = id2Degree.begin(); it!=id2Degree.end(); ++it)
        {
            degree2PrevID.insert(pair<int,int>(it->second, it->first));
        }
        id2Degree.clear();

        // {// degree test
        //     cout << "degree -> oldID" << endl;
        //     for (auto iter = degree2PrevID.begin(); iter != degree2PrevID.end(); iter ++) {
        //         cout << iter->first << ", " << iter->second << endl;
        //     }
        // }


		// newDegree2NewID; (i.e., id2Degree)
		// prevID2newID;
        this->OldID2NewID.clear();
        this->index2Degree.clear();
        int idCurrent = 0;
        for (auto iter = degree2PrevID.begin(); iter != degree2PrevID.end(); iter ++) {
            this->OldID2NewID.insert(pair<int,int>(iter->second, idCurrent));
            this->index2Degree.push_back(iter->first);
            idCurrent ++;
        }

        // {// print debug
        //     cout << "oldID -> newID" << endl;
        //     for (auto iter = OldID2NewID.begin(); iter != OldID2NewID.end(); iter ++) {
        //         cout << iter->first << "->" << iter->second << endl;
        //     }
        // }
        // cout << "old edge -> new edge" << endl;

		// sequentially read prev edge file and output new edge into new edgefile
        ifstream fp_oriEdgeFile(dataPath, ios::in);
        FILE * fp_NewIDEdgeFile = fopen(this->newEdgeFile.c_str(), "wb");
        if(fp_NewIDEdgeFile == NULL){
            cout << "error open fp_NewIDEdgeFile" << endl;
            abort();
        }
        FILE * fp_NewIDEdgeFileBackup = fopen(this->newEdgeFileBackup.c_str(), "wb");
        if(fp_NewIDEdgeFileBackup == NULL){
            cout << "error open fp_NewIDEdgeFileBackup" << endl;
            abort();
        }
        ofstream fp_NewIDEdgeTxt(this->newEdgeFileTxt);
        string _buf;
        int test_count = 0;
        while(getline(fp_oriEdgeFile, _buf)){
            if(_buf.size() < 1) {
                break;
            }
            stringstream _ss;
            _ss << _buf;
            int s = 0, d = 0;
            _ss >> s >> d;
            int newS = OldID2NewID[s];
            int newD = OldID2NewID[d];
            //cout << "old edge " << s << " -> " << d << endl;
            //cout << "new edge " << newS << " -> " << newD << endl;
            fwrite(&newS, sizeof(int), 1, fp_NewIDEdgeFile);
            fwrite(&newD, sizeof(int), 1, fp_NewIDEdgeFile);
            //cout << "write in newEdge " << newS << " " << newD << endl;
            fwrite(&newS, sizeof(int), 1, fp_NewIDEdgeFileBackup);
            fwrite(&newD, sizeof(int), 1, fp_NewIDEdgeFileBackup);
            //cout << "write in newEdgeBackup " << newS << " " << newD << endl;
            fp_NewIDEdgeTxt << newS << " " << newD << endl;
            test_count ++;
        }
        cout << "test_count = " << test_count << endl;
        fclose(fp_NewIDEdgeFile);
        fclose(fp_NewIDEdgeFileBackup);
        fp_NewIDEdgeTxt.close();
        fp_oriEdgeFile.close();

        // {//debug to save dgree
        //     ofstream it(this->tempFile + "degree.txt");
        //     int newID = 0;
        //     for(auto iter = this->index2Degree.begin(); iter != index2Degree.end(); iter++){
        //         it << newID << " " << *iter << endl;
        //         newID ++ ;
        //     }
        //     cout << "save newID -> degree" << endl;
        // }
        
		// sequentially read new edgefile and write into bNum sorted blocks
        FILE * fp_newEdgeBackup = fopen(this->newEdgeFileBackup.c_str(), "rb");
        if(fp_newEdgeBackup == NULL){
            cout << "write into bNum blocks open file error " << this->newEdgeFileBackup.c_str() << endl;
            abort();
        }
        //cout << "bucket sort" << endl;
        multimap<int, int> blockEdgeSorted;
        int edgeCount = 0;
        int bNum = 0;
        vector<int>bSize;
        while(edgeCount < this->edgeNum){
            //cout << "now in bNum" << bNum << endl;
            int count = 0;
            int src, des;
            
            while(count < this->mergeSortMaxEdge && edgeCount < this->edgeNum){
                if(fread(&src, sizeof(int), 1, fp_newEdgeBackup) == 0)break;
                fread(&des, sizeof(int), 1, fp_newEdgeBackup);
                blockEdgeSorted.insert(pair<int,int>(src, des));
                //cout << src << " " << des << endl;
                count++;
                edgeCount++;
            }
            string tempSortFile = this->tempSortFilePrefix + to_string(bNum) + ".dat";
            string tempSortFileTxt = this->tempSortFilePrefix + to_string(bNum) + ".txt";
            ofstream txt(tempSortFileTxt, ios::out);
            FILE * fp = fopen(tempSortFile.c_str() ,"wb");
            if(fp != NULL)
            {
                for(multimap<int,int>::iterator iter = blockEdgeSorted.begin(); iter != blockEdgeSorted.end(); iter++){
                    fwrite(&iter->first, sizeof(int), 1, fp);
                    fwrite(&iter->second, sizeof(int), 1, fp);
                    txt << iter->first << " " << iter->second << endl;
                }
                bSize.push_back(count);
                //cout << "index " << bNum << " size: " << count << endl;
                fclose(fp);
            }
            txt.close();
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
        ofstream txt(newEdgeFileTxt, ios::out);
        FILE * bFileList [bSize.size()] ={nullptr};
        int minSrcInPartition[bSize.size()] ={0};
        //cout << "SortFile size is " << bSize.size() << endl;
        cout << "begin open SortFile" <<endl;
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
        cout << "all file open " << endl;
        for(int i = 0; i < edgeNum; i++){
            //cout << "edgeIndex selected : " << i << endl;
            int src = INT_MAX;
            // if (i >= edgeNum - 3)
            //     cout << "src=" << src << endl;
            int blockIndex = -1;
            // if (i >= edgeNum - 3)
            //     cout << "bSize size-=" << bSize.size() << endl;//
            for(int k = 0; k < bSize.size(); k++){
                //cout << current[k] << " ";
                // if (i >= edgeNum - 3)
                //     cout << "k=" << k << ", minSrcInPartition[k]=" << minSrcInPartition[k] << endl;
                if(minSrcInPartition[k] < src && minSrcInPartition[k] != -1){
                    src = minSrcInPartition[k];
                    blockIndex = k;
                    // if (i >= edgeNum - 3)
                    //     cout << "k set blockIndex = " << k << endl;
                }
            }
            int des;
            fread(&des, sizeof(int), 1, bFileList[blockIndex]);
            // if (i >= edgeNum - 3)
            //     cout << "after feak" << endl;
            bSize[blockIndex]--;
            fwrite(&src, sizeof(int), 1, fp_newEdge);
            fwrite(&des, sizeof(int), 1, fp_newEdge);
            txt << src << " " << des << endl;
            //cout << src << " " << des << endl;
        
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
        txt.close();
        fclose(fp_newEdge);
        cout << "sort finish" << endl;
        // string cmd = "cd " + tempSortFilePrefix;
        // system(cmd.c_str());
        // cmd = "rm -rf *";
        // system(cmd.c_str());
	}

	void buildPartitionGap() {
        int degree = 0;
        int begin = 0;
        int index;
        for(index = 0; index < maxVertexID; index++){
            degree += index2Degree[index];
            if(degree > partitionSize){
                partitionGap.push_back(begin);
                degree = index2Degree[index];
                begin = index;
            }
        }
        if(partitionGap[partitionGap.size() - 1] != maxVertexID - 1){
            partitionGap.push_back(maxVertexID - 1);
        }

        // {
        //     cout << "PartitionGap is ";
        //     for(auto item : partitionGap){
        //         cout << item << " ";
        //     }
        //     cout << endl;
        // }
        
	}

	void buildPartitionFile() {
        // string cmd = "cd /media/data/vend/triangleCount/PartitionFile/";
        // system(cmd.c_str());
        // cmd = "rm -rf *";
        // system(cmd.c_str());
        cout << "PartitionFile build" << endl;
		FILE** fp_partList = new FILE*[partitionGap.size()];
		for (int i = 0; i < partitionGap.size(); i ++) {
			FILE* fp = fp_partList[i];
            string path = partitionDir + to_string(i) + ".dat";
			fp = fopen(path.c_str(),"ab");
            fp_partList[i] = fp;
		}
		// sequentially read new edge file, (s, d)  d find partitionID by getPartitionIn, fp_partList[partID]==write;
				// IDasSrcInPartitionList
        cout << "build Partition class" << endl;
        FILE * fp_in = fopen(newEdgeFile.c_str(), "rb");
        for(int i = 0; i < this->partitionGap.size(); i ++){
            int end = 0;
            if(i != this->partitionGap.size() -1) end = this->partitionGap[i + 1] -1;
            else end =  this->maxVertexID - 1;
            Partition partition(i ,this->partitionGap[i] ,end);//this->partitionGap[i] means the partition begin
            CompanionFile cfile;
            cfile.filename = this->companionFilePath + to_string(i) + ".dat";
            cfile.partitionID = i;
            partition.cfile = cfile;
            partitionList.push_back(partition);
        }
        cout << "build Partition File" << endl;
        for(int i = 0; i < this->edgeNum; i++){
            if(i % 100000 == 0)cout << i << endl;
            int src, des;
            fread(&src , sizeof(int), 1, fp_in);
            fread(&des , sizeof(int), 1, fp_in);
            int partitionIndex = getPartitionIn(des);
            string path = this->partitionDir + to_string(partitionIndex) + ".txt";
            ofstream fp_out(path,ios::app|ios::out);
            addIDasSrcInPartition(src, partitionIndex);
            fwrite(&src, sizeof(int), 1, fp_partList[partitionIndex]);
            fwrite(&des, sizeof(int), 1, fp_partList[partitionIndex]);
            partitionList[partitionIndex].edgeNum ++ ;
            fp_out << src << " " << des << endl;
            fp_out.close();
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
    Dataset dataset;
    //para inti
	dataset.edgeNum = 50888414;//22190596;//22190596;//38;
    //22190580
	dataset.maxVertexID = 1791489;//1696415;//1696415;//10;
	dataset.partitionNum = 5;//5;
	dataset.partitionSize = ceil(dataset.edgeNum / dataset.partitionNum);
    cout << "partitionSize : " << dataset.partitionSize << endl;
    clock_t start, preprocess, buildCFile, tc,finish;
    dataset.mergeSortMaxEdge = 1310720;//1310720;//2;
    dataset.triangle = 0;
    dataset.companionFilePath = "/media/data/vend/triangleCount/companyFile/";
    dataset.newEdgeFile = "/media/data/vend/triangleCount/newGraph/newEdge.dat";
    dataset.newEdgeFileBackup = "/media/data/vend/triangleCount/newGraph/newEdgeBackup.dat";
    dataset.partitionDir = "/media/data/vend/triangleCount/PartitionFile/";
    dataset.dataPath = "/media/data/vend/triangleCount/LastONE/wiki-shuf.txt";
    dataset.tempSortFilePrefix = "/media/data/vend/triangleCount/SortFile/";
    dataset.newEdgeFileTxt = "/media/data/vend/triangleCount/newGraph/newEdge.txt";
    dataset.tempFile = "/media/data/vend/triangleCount/tempFile/";
    start = clock();
    cout << "begin preprocess" << endl;
    dataset.preprocess();
    cout << "preprocess finish()" <<endl;
    preprocess = clock();
    cout << "use time :" << (double)(preprocess - start)/CLOCKS_PER_SEC << " s" << endl;
    dataset.buildCompanionFile();
    cout << "buildCompanionFile finish()" <<endl;
    buildCFile = clock();
    cout << "use time :" << (double)(buildCFile - preprocess)/CLOCKS_PER_SEC << " s" << endl;
    dataset.triangleCount();
    tc = clock();
    cout << "use time :" << (double)(tc - buildCFile)/CLOCKS_PER_SEC << " s" << endl;
    cout << "tc finish()" <<endl;
    cout << "preprocess use time :" << (double)(preprocess - start)/CLOCKS_PER_SEC << " s" << endl;
    cout << "buildCompanionFile time :" << (double)(buildCFile - preprocess)/CLOCKS_PER_SEC << " s" << endl;
    cout << "tc time :" << (double)(tc - buildCFile)/CLOCKS_PER_SEC << " s" << endl;
    cout << "total time: " << (double)(tc - start)/CLOCKS_PER_SEC << " s" << endl;
    cout << "triangele count: " << dataset.triangle<<endl;
    return 0;
}