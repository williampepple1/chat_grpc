@echo off
echo Generating gRPC Go files...
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/chat.proto
if %ERRORLEVEL% neq 0 (
    echo Generation failed!
    exit /b %ERRORLEVEL%
)
echo Generation successful!
