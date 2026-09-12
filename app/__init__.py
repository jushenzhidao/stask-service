"""stask-service：同步转异步任务网关。

**服务版本号的唯一事实源在本文件。** ``__version__`` 是全仓唯一一处手写的版本号：

- :data:`app.config.Settings.app_version` 的默认值直接引用它（仍可用 ``APP_VERSION``
  env 覆盖——生产通常由镜像 tag 注入）；
- ``pyproject.toml`` 用 ``dynamic = ["version"]`` + ``[tool.setuptools.dynamic]``
  的 ``attr`` 从它派生，故**打包元数据与运行时永远一致**，不会出现
  「pyproject 写 0.1.0、config 写 0.2.0、镜像 tag 是 0.4.0」这种三头马车。

改版本只改下面这一行。约束由
``tests/test_spec_contract.py::test_service_version_has_single_source`` 机械守住。

**注意：本模块禁止 import 任何应用内模块**——它被 ``app.config`` 在导入期引用，
这里一旦反向导入就构成循环导入（且症状是启动时莫名 ImportError，不好查）。
"""

__version__ = "0.4.0"
