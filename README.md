# Triangle Counting in Disk
In this paper, we introduce novel acceleration strategies for disk-based triangle counting, a core problem in graph computation. Due to memory constraints, existing in-memory algorithms struggle with large graphs stored on disk. External frameworks mitigate this by partitioning the graph and creating tuples of neighborhood information for sequential loading and triangle counting. However, these methods often incur substantial I/O overhead due to the large number and size of tuples.   
Our approach improves upon these frameworks in two ways. Firstly, we significantly reduce the space cost for storing each tuple. Secondly, we decrease the number of tuples written to disk. These strategies not only save disk space but also improve in-memory computation time.   We validate our methods through comprehensive experiments on real-life datasets. The experimental results show that our method takes only 40\% of the time of the existing methods. Particularly in the algorithm's I/O time, we only take 10\% of the time compared to the existing methods, demonstrating our approach's improved effectiveness.

# Run
```
mkdir build
cmake ..
make
./BBFTC
```