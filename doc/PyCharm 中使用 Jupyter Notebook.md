## PyCharm 中使用 Jupyter Notebook

### 参考链接: 
```text
https://www.pythonheidong.com/blog/article/148295/
```

### 安装 jupyter notebook
```text
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple jupyter notebook
```

### 安装完成后在 Terminal 命令行输入 "jupyter notebook". 启动服务. 
获取 URL 和 token 如: http://localhost:8889/?token=1eaeb2df33bb9c0f6219aff8c83220abe404e4431e409a6b
有了这个, 就可以在 PyCharm 中运行 ipynb 文件了. 

如下: 
```text
(PySpark) D:\Users\Administrator\PycharmProjects\PySpark>jupyter notebook
[I 23:24:33.815 NotebookApp] The port 8888 is already in use, trying another port.
[I 23:24:33.824 NotebookApp] Serving notebooks from local directory: D:\Users\Administrator\PycharmProjects\PySpark
[I 23:24:33.825 NotebookApp] The Jupyter Notebook is running at:
[I 23:24:33.825 NotebookApp] http://localhost:8889/?token=1eaeb2df33bb9c0f6219aff8c83220abe404e4431e409a6b
[I 23:24:33.825 NotebookApp]  or http://127.0.0.1:8889/?token=1eaeb2df33bb9c0f6219aff8c83220abe404e4431e409a6b
[I 23:24:33.825 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 23:24:33.831 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///C:/Users/Administrator/AppData/Roaming/jupyter/runtime/nbserver-816-open.html
    Or copy and paste one of these URLs:
        http://localhost:8889/?token=1eaeb2df33bb9c0f6219aff8c83220abe404e4431e409a6b
     or http://127.0.0.1:8889/?token=1eaeb2df33bb9c0f6219aff8c83220abe404e4431e409a6b
[I 23:24:57.299 NotebookApp] 302 GET / (127.0.0.1) 0.00ms
[I 23:24:57.301 NotebookApp] 302 GET /tree? (127.0.0.1) 0.00ms
[I 23:24:57.380 NotebookApp] Kernel started: 5693babe-da74-483b-b0d8-19ed25ff1e5c
[W 23:24:57.428 NotebookApp] No session ID specified
```
