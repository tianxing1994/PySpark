## PySpark 安装问题



### 安装
```text
练习时使用本地文件, 所以不需要使用 Hadoop 数据库. 
读取本地文件时, 文件路径的构造方式如下(windows 系统): 
file:///D:/Users/Administrator/PycharmProjects/PySpark/dataset/test.txt
```

### 异常
问题:   
```text
用户警告：请安装psutil以更好地支持溢出  
```
表现: 程序输出中出现: 
```text
UserWarning: Please install psutil to have better support with spilling
```
解决办法: 
```text
pip install -U -i https://pypi.tuna.tsinghua.edu.cn/simple psutil
```
理解: 
```text

```
