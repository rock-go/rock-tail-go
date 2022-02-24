# tail
文件读取模块 按行读取配置文件 并进行回调处理

## rock.tail
- [userdata = rock.tail{name , limit , thread , to}](#内部方法)
- [userdata = rock.tail(name)](#内部方法)
- name: 服务进程名称
- limit: 限速器
- thread: 采集的线程
- to: 文件传输
```lua
    local t = rock.tail{
        name = "tail",
        limit = 100 ,       -- 100条/s
        thread = 2 ,        -- 处理线程  2/thread
        to     = lua.writer -- 数据传输
    }
    t.start()

    local t = rock.tail("tail")
    --todo
    t.start()
```
### 内部方法

- [userdata.pipe(value)]()
- [userdata.add(codec , value)]()
- [userdata.thread(int)]()
- [userdata.limit(int)]()
- [userdata.to(lua.writer]()
- [userdata.json(value)]()
- [userdata.file(path) fx](#文件处理)
- [userdata.dir(dir , grep) dx](#目录处理)
- [userdata.start()]()
- [完整样例](#example)

### 文件处理
- 处理单个文件 提供了 断点缓存 follow pipe 等处理方法
- [fx.path]()
- [fx.wait(true)]()
- [fx.bkt(string , string,...)]()
- [fx.node(codec)]()
- [fx.json(value)]()
- [fx.add(codec , value)]()
- [fx.delim(byte)]()
- [fx.buffer(int)]()
- [fx.pipe(value)]()
- [fx.on(tx)](#内部函数)
- [fx.run()]()

[fx.on(tx)]() 是当触发文件读取的EOF是触发事件 [tx](内部结构) 内部变量函数<br/>
[fx.delim]() 分隔符号 默认：'\n' <br/>
[fx.node(codec)]() 默认添加节点信息 同样只要[json]() 和 [raw]() 两种状态模式 <br/>
[fx.add]()优先级大于tail模块

### 目录处理
- 读取一个目录下多个文件 并启用匹配符号
- [dx.poll(interval , dead)]()
- [dx.inotify(deadline)]()
- [dx.on(fx)]()
- [dx.run()]()

[dx.poll(10 , 3600 )]() 是采用周期轮询方式监控文件夹监听 如果会自动匹配增删改 [dead]()可选 监控时长 <br/>
[dx.inotify(3600 )]() 是采用inotify监控文件加 判断write或者delete事件 [dead]() 可选 监控时长<br/>
```lua
    local dx = t.dir("/var/log" , "*.log")
    dx.poll(100) -- 100s/次
    dx.on(function(fx)
        print(fx.path) 
        fx.wait(true)
        fx.buffer(10000)
        fx.pipe()
        fx.node("json")
        fx.on(function(tx) tx.poll(100) end)
        fx.run()
    end)
    dx.run()
```

### 内部函数
用来处理file文件处理的时候发生EOF是 手动处理逻辑 Tx变量

- [tx.time]() &nbsp; 监控启动时间 
- [tx.exit()]() &nbsp; 关闭监控
- [tx.poll(time , dead)]() &nbsp; 使用周期监控的方式 监听文件变化
- [tx.inotify(dead)]() &nbsp; 使用文件监控事件监控变化
- [tx.after(time)]()   &nbsp; EOF发生多久后触发监控 防止读取过快导致过快启动监控
- [tx.rename(format , ....)]() &nbsp; 修改文件名

## example
```lua

local t = rock.tail("error").limit(10).to(k).start()

local function on(tx)
  tx.after(3)
  tx.inotify()
end

local function show(raw)
  std.out.println(raw)  
end

local dx = t.dir("logs" , "xss*.log")
dx.poll(1)
dx.on(_(fx)
  fx.bkt("tail_seed_record_dir")
  fx.node("json")
  fx.pipe(show)
  fx.on(on)
  fx.run()
end)
dx.run()

local fx = t.file("logs\\xss.log")
fx.wait(true)
fx.bkt("tail_seed_record_6")
fx.node("json")
fx.pipe(show)
fx.on(on)
fx.run()
```