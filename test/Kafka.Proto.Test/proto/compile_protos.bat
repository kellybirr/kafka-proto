@rem Generate the C# code for .proto files

setlocal

@rem enter this directory
cd /d %~dp0

set TOOLS_PATH=C:\Users\%USERNAME%\.nuget\packages\grpc.tools\1.15.0\tools\windows_x64
set PROJECT=..

%TOOLS_PATH%\protoc.exe --proto_path=. --csharp_out=%PROJECT% --grpc_out=%PROJECT% --plugin=protoc-gen-grpc=%TOOLS_PATH%\grpc_csharp_plugin.exe testMessages.proto

endlocal
