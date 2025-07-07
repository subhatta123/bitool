; ConvaBI Installer Script for Inno Setup
; This script creates a professional, self-contained installer for the ConvaBI application.

[Setup]
; --- Application Identity ---
AppId={{ConvaBI-App-ID}}
AppName=ConvaBI
AppVersion=2.0
AppVerName=ConvaBI 2.0
AppPublisher=ConvaBI Project
AppPublisherURL=https://github.com/
AppSupportURL=https://github.com/
DefaultDirName={autopf}\ConvaBI

; --- Installer Settings ---
DefaultGroupName=ConvaBI
DisableProgramGroupPage=yes
OutputDir=installer
OutputBaseFilename=ConvaBI_v2_Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
WizardSmallImageFile=compiler:WizModernSmallImage.bmp

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; --- Main Application Files ---
; This command recursively includes all files from the PyInstaller output directory ('dist/ConvaBI').
Source: "dist\ConvaBI\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; --- Start Menu and Desktop Icons ---
Name: "{group}\ConvaBI"; Filename: "{app}\ConvaBI.exe"
Name: "{group}\{cm:UninstallProgram,ConvaBI}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\ConvaBI"; Filename: "{app}\ConvaBI.exe"; Tasks: desktopicon

[Run]
; --- Post-Installation ---
; Launch the application after the installation is complete.
Filename: "{app}\ConvaBI.exe"; Description: "{cm:LaunchProgram,ConvaBI}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; --- Uninstallation ---
; This ensures that the entire application directory is removed on uninstall.
Type: filesandordirs; Name: "{app}" 