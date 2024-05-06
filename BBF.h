#include <bitset>
#include <iostream>
#include <vector>
#include <fstream>
#include <cmath>
using namespace std;
class BBF
{
public:
    void setBBF(unsigned long long int _bitSetSize, int _hashNum)
    {
        bitSet.resize(_bitSetSize);
        cout << "after resize bitSet's size is " << bitSet.size() << endl;
        hashNum = _hashNum;
        bitSetSize = _bitSetSize;
    }
    bool NEpairTest(int vertex, vector<int> iAdj);
    bool setBitSet(int vertexSrc, int vertexDes);
    void showBitSet();
    void saveBitSet(string FilePath, string dataPath);

private:
    unsigned long long int getPos(int src, int des, int hashParamIndex);
    int vertexNum;
    vector<bool> bitSet;
    // vector<int>hashPara = {10111,10117,101149,101203,101197, 101209, 101221};
    vector<int> hashPara = {1000003, 1000033, 1000037, 1000039, 1000081, 1000099, 1000117};
    int hashNum;
    unsigned long long int bitSetSize;
    static bool isPrime_3(int num)
    {
        if (num == 1)
            return 0;
        if (num == 2 || num == 3)
            return 1;
        if (num % 6 != 1 && num % 6 != 5)
            return 0;
        int tmp = sqrt(num);
        for (int i = 5; i <= tmp; i += 6)
            if (num % i == 0 || num % (i + 2) == 0)
                return 0;
        return 1;
    }
};