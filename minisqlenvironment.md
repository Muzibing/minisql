配置环境完成 2024.4.21
我使用的是vscode+wsl的形式
part1：配置wsl
    wsl之前已经配置好，网上教程不少
part2：clone代码框架
    进入vscode主页打开wsl：ubuntu
    打开终端 在zju内网下 ```sudo git clone https://git.zju.edu.cn/zjucsdb/minisql.git```
    以下各步骤如果提示没有权限的话就加一个```sudo``` 然后输入密码就可以啦
    ```cd minisql```
    ```mkdir build```
    ```cd build```
    ```sudo cmake ..```
    在这几步我都没有遇到问题
part3：修改一下框架
    在我的测试中，如果直接按语雀上输入```make -j4```会提示报错，具体是_cv变量找不到
    这个时候如果直接修改/minisql/src/include/concurrency/lock_manager.h
    为它添加头文件```#include<condition_variable>```会提示没有保存权限，无法修改
    我尝试过使用vim相关的指令但没有成功
    解决方法是通过```cd xx```进入项目根目录（即一步步进入concurrency文件夹）
    采用```sudo chmod 777 -R lock_manager.h```获得保存权限，之后就可以修改了
    助教gg在钉钉群内说修改过后即可通过，但我这里还出现了问题，是/executor/plans/abstract_plan.h的智能指针缺少头文件
    这个时候我先进入根目录，修改保存权限，加入了```#include<memory>```然后再编译就解决了问题！
part4：完成
    采用```sudo make -j4```完成配置，我这里是没有报错啦
    终端输入```./bin/main```回车就弹出minisql了，不过只能quit，因为还没写别的操作它不认识
part5：测试
    进入build文件夹，```make lru_replacer_test```，还是如果没权限就开sudo
    还是build目录下，```./test/lru_replacer_test```就可以测试~

怎么把zjugit的代码弄到github自己的仓库里
https://blog.csdn.net/m0_55546349/article/details/121786789
如何提交本地的更改到那个仓库
git add .
git commit -m "commit message" commit message就是批注
git push origin main

此外：安装clangd可以避免全是红色9+的错误 方便调试和看着舒心
步骤如下：
https://zhuanlan.zhihu.com/p/592802373

完成：
    4.26    开始尝试#1 1.2 disk_manager_test bitmap_page.cpp
    5.6     #1 1.2 disk_manager_test 测试点1通过 开始尝试#1 1.3 disk_manager_test disk_manager.cpp
    5.10    #1 完成disk_manager.cpp 开始尝试#1 1.4 lru_replacer_test lru_replacer.cpp
    5.15    #1 1.4 lru_replacer_test 测试点通过 开始尝试#1 1.5 buffer_pool_manager_test buffer_pool_manager.cpp
    5.16    #1 1.5 buffer_pool_manager.cpp完成 不过似乎没有过测试点 不管了开始#2 2.1
    5.31    #2 2 程序全部完成 但是测试点一个都没过 tuple_test和table_heap_test
    6.2     #2 2.1 tuple_test测试点通过
    6.3     #1 disk_manager_test 测试点2通过 是之前环境的问题 在mnt目录下重新配了下 之前是直接wsl的目录下
               发现过程：使用cerr对不通过的测试点进行排查 发现是"xx.db"那句话的导入出了问题 在网上复制了正确同学的代码 还是这个地方有问题 观看同学正确的环境 发现有区别 于是开始尝试新环境
            差 #1 buffer_pool_manager_test和#2 table_heap_test
    6.4     #1 buffer_pool_manager测试点通过 #2 table_heap_test测试点通过
    6.6     #3 b_plus_tree_index_test测试点和index_iterator_test测试点通过 b_plus_tree_test查询那一块有问题