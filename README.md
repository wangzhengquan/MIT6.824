 # MIT6.5840 / 6.824 - Spring 2023

 

##  Linearizability and Serializability 
- “Linearizability” 是指一个client在多个servers（这些servers上会运行raft/paxos等一致性算法以实现faulty tolerant）上执行命令就像在一个server上一样，Linearizability是一对多的关系。比如client C向server端发送一系列的命令`i_1, i_2, i_3 ...`, 在对方某些机器可能会出现故障的情况下，这些命令应该按顺序执行，而且不会重复执行。Linearizability的基础是raft/paxos等一致性（Consensus）算法。Lab3 Lab4都是基于lab2实现的raft实现一个map读写的Linearizability，Lab3实现了map在同一个goup里的Linearizability，Lab4实现了map跨多个goups的Linearizability.

- “Serializability” 是指多个clients在一台server或一个cluster上执行命令的时候是线程安全的不会出现race condition， Serializability是多对一的关系. 比如server S上的一个变量a=100, 两个client C1,C2 同时读取和修改变量a, C1 add(a, 10), C2 sub(a, 10), 最终a的值还是100不会是其他值。实现Serializability的基础是"lock service"。该课程的实验没有涉及到Serializability。


## 课程资源
- Websit： https://pdos.csail.mit.edu/6.824/
- Video： https://www.bilibili.com/video/BV16f4y1z7kn/

## Raft 介绍
- https://raft.github.io/
- Visualizations : http://thesecretlivesofdata.com/raft/
- https://www.usenix.org/conference/atc14/technical-sessions/presentation/ongaro

## students-guide
students-guide-to-raft: https://thesquareplanet.com/blog/students-guide-to-raft/

## Raft implementation In C++
- https://github.com/logcabin/logcabin
- https://github.com/tjumcw/6.824