; DBChat Installer Script for Inno Setup
; This script creates a professional, self-contained installer for the DBChat application.

[Setup]
; --- Application Identity ---
AppId={{DBChat-App-ID}}
AppName=DBChat
AppVersion=2.0
AppVerName=DBChat 2.0
AppPublisher=DBChat Project
AppPublisherURL=https://github.com/
AppSupportURL=https://github.com/
DefaultDirName={autopf}\DBChat

; --- Installer Settings ---
DefaultGroupName=DBChat
DisableProgramGroupPage=yes
OutputDir=installer
OutputBaseFilename=DBChat_v2_Setup
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
; This command recursively includes all files from the PyInstaller output directory ('dist/DBChat').
Source: "dist\DBChat\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; --- Start Menu and Desktop Icons ---
Name: "{group}\DBChat"; Filename: "{app}\DBChat.exe"
Name: "{group}\{cm:UninstallProgram,DBChat}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\DBChat"; Filename: "{app}\DBChat.exe"; Tasks: desktopicon

[Run]
; --- Post-Installation ---
; Launch the application after the installation is complete.
Filename: "{app}\DBChat.exe"; Description: "{cm:LaunchProgram,DBChat}"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; --- Uninstallation ---
; This ensures that the entire application directory is removed on uninstall.
Type: filesandordirs; Name: "{app}" 