#include "BBF.h"
using namespace std;
bool BBF::NEpairTest(int vertex, vector<int> iAdj){//true->filter 
    //cout << "NEpairTest src: " << vertex << endl;
    int falseCount = 0;
    for(int i = 0; i < iAdj.size(); i++){
        int flag = 0;
        for(int hashID = 0; hashID < this->hashNum; hashID++){
            if(this->bitSet[this->getPos(vertex, iAdj[i], hashID)] == false){
                flag = 1;
                falseCount ++;
                break;
            }
        }
        if(flag == 0)//at least one edge all correct
        {
            return false;
        }
    }
    if(falseCount == iAdj.size())return true;
    else return false;
}
void BBF::showBitSet(){
    for(int i = 0; i < this->bitSetSize; i++){
        cout << this->bitSet[i] << " ";
    }
    cout << endl;
}
bool BBF::setBitSet(int vertexSrc, int vertexDes){
    //cout << "build bitset src: " << vertexSrc << " des: " << vertexDes << endl;
    for(int i = 0; i < this->hashNum; i++){
        unsigned long long int pos = this->getPos(vertexSrc, vertexDes, i);
        this->bitSet[pos] = true;
    }
    return true;
}

unsigned long long int BBF::getPos(int src, int des, int hashParaIndex){
    return ((unsigned long long int)des  * this->hashPara[hashParaIndex] + src) % this->bitSetSize;
}

void BBF::saveBitSet(string FilePath, string dataPath){
    unsigned long long int oneCount = 0;
    unsigned long long int zeroCount = 0;
    ofstream fp(FilePath , ios::out);
    for(int i = 0; i < this->bitSetSize; i++){
        if(this->bitSet[i] == true){
            oneCount++;
        }
        else{
            zeroCount++;
        }
        fp << this->bitSet[i] << endl;
    }
    fp.close();
    double onePercent = oneCount * 100 /this->bitSetSize;
    double zeroPercent = zeroCount * 100 /this->bitSetSize;
    ofstream fp_data(dataPath, ios::out);
    fp_data << "one's Percent is " << to_string(onePercent) << endl;
    fp_data << "zero's Percent is " << to_string(zeroPercent) << endl;
}