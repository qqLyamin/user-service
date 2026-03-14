@echo off
setlocal
call "C:\Program Files\Microsoft Visual Studio\18\Insiders\VC\Auxiliary\Build\vcvars64.bat"
if errorlevel 1 exit /b 1
if not exist "%~dp0..\build" mkdir "%~dp0..\build"
cl /nologo /std:c++20 /EHsc /W4 /O2 /Fe:"%~dp0..\build\user-service.exe" "%~dp0..\cmd\user-service\main.cpp" ws2_32.lib
