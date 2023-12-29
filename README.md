 # MIT6.5840 / 6.824 - Spring 2023

 ## 个人感悟
 程序员信奉一句话“程序是调出来的”，对于一些简单的程序and/or一些显而易见的bug确实是这样的，但是在一些算法特别复杂的，and/or系统架构复杂的，and/or多线程的，and/or分布式的程序来中所出现那些很微妙的偶尔复现的Bug，你几乎不可能再用debug的方式去排除错误，而且你也几乎无法在海量的log中找到有价值的线索。其实大多数程序猿都犯了一个认知错误，认为是自己在调试代码，其实程序代码只是人的思维在计算机上的呈现，当程序出现了微妙的bug就证明人的思维出现了逻辑上的纰漏，这个时候我们应该不断的检视自己思维上的纰漏直到思维上觉察到那个纰漏，再尝试修复代码并测试，不断的重复这个过程直到自己的思维完全是自洽的再也没有逻辑缺陷的时候，你的程序也就没有bug了。 也就是说计算机只是辅助程序员寻找自己思维逻辑上的漏洞的工具，我们不是在调试和修复计算机程序的bug而是在调试和修复自己的思维漏洞，我们是计算机的主人不是奴隶。关于本课程的labs我有参考网上的一些实现，但是我发现这些实现代码混乱运行缓慢，看他们的实现代码更像是为了通过测试并针对测试拼凑出来的，这样的代码肯定是逻辑混乱的，所以它们虽然能通过单次测试，但是`dstest -p8 -n32 .`基本都过不了。这其实就是大多数程序猿犯的认知错误，让计算机牵着鼻子走，让自己成了计算机的奴隶。

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