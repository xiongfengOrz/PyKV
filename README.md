# PyKV
这是我用python写的一个KV数据库。现在已经完成基本功能，还需要多多完善。
### 基本特性
- 使用zeromq完成client端和server端的通信
- 支持client端的增删查改
- 支持存储引擎的替换，默认使用json存储数据 
- server端支持多种查询模式，详情见后面具体实例

#### 待完善
- 支持一致性哈希完成分布式存储


#### 开发环境

Python 2.7.6

#### 安装

PyKV依赖

   - [pyzmq](https://github.com/zeromq/pyzmq)
   
安装	

	    pip install -r requirements.txt
#### 用例
在客户端建立同server的链接，数据库名称db，建立三个空的query
     
```python

     
           client = client_factory("db")
           quer1 = Query()
           quer2 = Query()
           quer3 = Query()
```	   
##### 查询
```python
         client.search(quer1.name == "he")
 ```
##### 删除
```python
           client.remove(quer2.name == "he")
```
##### 插入

```python
           client.insert([{"name":"he"}])
```
##### 更新
```python
           client.update("delete", quer3.name == "he")
```
#### 运行

本机运行客户端
```python
           python client.py
```
	   
 本机运行服务端
 
```python
           python server.py
```
     
 Have fun！
