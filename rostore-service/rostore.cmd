@echo off
@setlocal

echo - RO-STORE CLI v1.0.0 ----------------------------
echo:

@rem --- Check or set the ROSTORE_HOME

if "%ROSTORE_HOME%" == "" set "ROSTORE_HOME=%CD%"

cd /d "%ROSTORE_HOME%"

if not "%ROSTORE_HOME%" == "%CD%" (
  echo ^> Error: The ROSTORE_HOME is not set or set to non-existing directory.
  goto :error
)

set JAR_FILE=rostore-service.jar
set "CODE_BASE=%ROSTORE_HOME%\"
if exist "%ROSTORE_HOME%\target" set CODE_BASE="%ROSTORE_HOME%\target\"

set "ROSTORE_JAR_FILE=%CODE_BASE%%JAR_FILE%"
set CLI_CLASS=org.rostore.org.rostore.cli.Cli
set "JAVA_CLI=java -cp %ROSTORE_JAR_FILE% %CLI_CLASS%"

if not exist "%ROSTORE_JAR_FILE%" (
  echo ^> Error: No %ROSTORE_JAR_FILE% file found in ROSTORE_HOME
  goto :error
)

@rem --- Read the rostore.properties and set them as variable

echo   ROSTORE_HOME=%ROSTORE_HOME%

for /F "eol=# delims== tokens=1,*" %%a in (%ROSTORE_HOME%\rostore.properties) do (
    if NOT "%%a"=="" if NOT "%%b"=="" call :setVar %%a %%b
)

set "NOT_SET_KEY=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

echo:

if "%ROSTORE_ROOT_API_KEY%" == "%NOT_SET_KEY%" (
  echo ^> Error: Please, generate a valid uuid and replace ROSTORE_ROOT_API_KEY=%ROSTORE_ROOT_API_KEY% by this uuid.
  goto :error
)

if "%ROSTORE_PUBLIC_API_KEY%" == "%NOT_SET_KEY%" (
  echo ^> Error: Please, generate a valid uuid and replace ROSTORE_PUBLIC_API_KEY=%ROSTORE_PUBLIC_API_KEY% by this uuid.
  goto :error
)

@REM --- Init logging

set ROSTORE_LOG_FILE=%ROSTORE_LOG_DIR%\rostore.log

if not exist "%ROSTORE_LOG_FILE%" (
  if not exist "%ROSTORE_LOG_DIR%\" (
    echo No log directory %ROSTORE_LOG_DIR% is detected. Create one.
    @setlocal enableextensions
    md %ROSTORE_LOG_DIR%
    @endlocal
  ) else (
    copy nul > %ROSTORE_LOG_FILE%
  )
)

@rem --- Check script parameters

set OPERATION=%1
set DEBUG=%2

set "OPERATION_LIST=stop, start, restart, log, log-tail, status"

if "%OPERATION%" == "" (
  echo Usage: %0 operation [-debug]
  echo Operation: %OPERATION_LIST%
  goto :error
)

@rem --- Main part: operations

if "%OPERATION%" == "status" (
  goto :status
) else if "%OPERATION%" == "stop"  (
  goto :stop
) else if "%OPERATION%" == "start" (
  echo ! Starting a rostore process
  echo:
  call :start
  goto :end
) else if "%OPERATION%" == "restart" (
  goto :restart
) else if "%OPERATION%" == "log-tail" (
  echo ! Logging the rostore process tail
  echo:
  echo Use: %0 log
  echo ^> Error: Unsupported on windows.
  goto :error
) else if "%OPERATION%" == "log" (
  set "ROSTORE_LOG_LINES=100"
  echo ! Showing last %ROSTORE_LOG_LINES% lines of rostore process log (%ROSTORE_LOG_FILE%)
  echo:
  type %ROSTORE_LOG_FILE%
  goto :end
) else (
  echo ^> Error: unknown operation %OPERATION%
  echo:
  echo Usage: %0 operation
  echo Operation: %OPERATION_LIST%
  goto :error
)

@rem -- Functions

:setVar
  set KEY=%1
  set VALUE=%2
  CALL SET CHANGED_VALUE=%%VALUE:$ROSTORE_HOME=%ROSTORE_HOME%%%
  if "%KEY:~-4%" == "_DIR" (
    set CHANGED_VALUE=%CHANGED_VALUE:/=\%
  )
  if "%KEY:~-5%" == "_FILE" (
    set CHANGED_VALUE=%CHANGED_VALUE:/=\%
  )
  set %KEY%=%CHANGED_VALUE%
  echo   %KEY%=%CHANGED_VALUE%
goto :eof

:detect_rostore
  set "ROSTORE_PID="
  for /f "tokens=*" %%g IN ('wmic process where caption^="java.exe" get processid^,commandline 2^>^&1 ^| find "java" ^| find "%ROSTORE_JAR_FILE%" ^| find "%ROSTORE_HOME%"') do for %%A in (%%~g) do SET ROSTORE_PID=%%A
goto :eof

:status
  echo ! Retrieving the rostore process status
  echo:
  call :detect_rostore
  if not "%ROSTORE_PID%" == "" (
    echo There is a running rostore process with PID=%ROSTORE_PID%..
    goto :end
  ) else (
    echo ^> Error: No rostore process is currently running..
    goto :error
  )

:stop
call :detect_rostore
  echo ! Stopping an existing rostore process
  echo:
  if not "%ROSTORE_PID%" == "" (
    echo Shutdown the running rostore process PID=%ROSTORE_PID%
    echo:
    %JAVA_CLI% shutdown
    if not errorlevel 0 (
      echo ^> Error: Shutdown did not work.
      goto :error
    )
    echo ^> Rostore process has been successfully terminated.
    goto :end
  ) else (
    echo ^> Error: No rostore process has been detected
    goto :error
  )

:start
  call :detect_rostore
  if not "%ROSTORE_PID%" == "" (
    echo ^> Error: There is another rostore process running, PID=%ROSTORE_PID%
    goto :error
  )
  if "%DEBUG%" == "-debug" (
    if "%ROSTORE_DEBUG_PORT%" == "" (
      set "ROSTORE_DEBUG_PORT=5005"
    )
    set "DEBUG=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:%ROSTORE_DEBUG_PORT%"
  ) else (
    set "DEBUG="
  )
   
  echo Starting a new rostore process, logging to %ROSTORE_LOG_FILE%
  if not "%DEBUG%" == "" echo Debug mode is enabled, attach debugger to %ROSTORE_DEBUG_PORT%
  start /b java %DEBUG% %ROSTORE_JAVA_OPTIONS% -jar %ROSTORE_JAR_FILE% >> %ROSTORE_LOG_FILE% 2>&1
  timeout 1 1>nul
  exit /b
  call :detect_rostore
  if not "%ROSTORE_PID%" == "" (
    echo:
    echo ^> Rostore has been successfully started, PID=%ROSTORE_PID%
    goto :eof
  ) else (
    echo ^> Error: No running version is detected. Error.
    goto :error
  )
  goto :eof

:restart
  call :detect_rostore
    echo ! Re-starting a rostore process
    echo:
    if not "%ROSTORE_PID%" == "" (
      echo There is another rostore process running, PID=%ROSTORE_PID%. Shut it down first.
      %JAVA_CLI% shutdown
      if not errorlevel 0 (
        echo ^> Error: Shutdown did not work.
        goto :error
      )
      call :restart2
    ) else (
      echo Warning: no other rostore process has been detected. Continue.
    )
  call :start
  goto :end

:restart2:
  call :detect_rostore
  if not "%ROSTORE_PID%" == "" (
    echo ^> Error: Shutdown did not work.
    goto :error
  )
  echo The old process has been successfully terminated.
  goto :eof

@rem -- Finals

:error
set ERROR_CODE=1

:end
@endlocal & set ERROR_CODE=%ERROR_CODE%

cmd /C exit /B %ERROR_CODE%