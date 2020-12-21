### idea添加scala支持
- 首先安装scala插件
- file-->Project Structure.... -->Global Libraries
   --> 点击+号添加scala sdk（可以下载需要的版本）--> 点击应用、确认
- 在项目名称上点击右键-->Add FrameWork Support... -->选中scala需要的版本-->确认
---
### idea添加运行参数
- run--> Edit Configurations... -->左侧选中项目-->点击Configuration
- program arguments处填写参数（比如-host localhost -port 9999）-->点击应用、确认
---
### flink运行时组件
- 作业管理器（JobManager）
- 资源管理器（ResourceManager）
- 任务管理器（TaskManager）
- 分发起（Dispatcher）
---
### idea中配置运行参数方法
- run-> Edit Configurations...
- -->Configuration标签页的Program arguments中 写参数，例如-host localhost -port 9999
---
某个算子的最大子并行度就是该流式任务的并行度
---
