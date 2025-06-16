@echo off
REM This script compiles the Inno Setup installer for DBChat.

REM Change to the directory where this script is located
cd /d "%~dp0"

echo Running PyInstaller to bundle the application...
pyinstaller DBChat.spec --noconfirm --clean

REM Check if the PyInstaller build was successful
if not exist "dist\DBChat\DBChat.exe" (
    echo.
    echo ERROR: PyInstaller failed to create the executable.
    echo The 'dist\DBChat' directory or its contents are missing.
    echo Please review the PyInstaller output above for errors.
    pause
    exit /b
)

echo.
echo PyInstaller bundling complete.
echo.

REM --- Inno Setup Compilation ---
SET ISCC="C:\Program Files (x86)\Inno Setup 6\iscc.exe"

if not exist %ISCC% (
    echo Inno Setup Compiler not found at %ISCC%
    echo Please install Inno Setup 6 from https://jrsoftware.org/isinfo.php
    pause
    exit /b
)

echo Compiling the DBChat installer...
%ISCC% "DBChat_installer.iss"

echo.
echo Installer creation complete.
echo The installer can be found in the 'installer' sub-directory.
pause 