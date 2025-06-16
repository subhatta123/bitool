import subprocess
import os
import sys
import platform
import time # Added for potential sleep

print("--- run_dbchat.py script started ---")
print(f"Python version: {sys.version}")
print(f"sys.executable (path to DBChat.exe when bundled): {sys.executable}")
print(f"sys.argv: {sys.argv}")
print(f"Current working directory: {os.getcwd()}")
print(f"PYI_PYTHON_EXE env var: {os.environ.get('PYI_PYTHON_EXE')}")
print(f"Frozen: {getattr(sys, 'frozen', False)}, _MEIPASS: {getattr(sys, '_MEIPASS', 'Not set')}")

def get_base_path():
    """ Get the base path for PyInstaller (frozen app) or normal script. """
    try:
        if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
            # _MEIPASS is the root of the extracted files in a bundle
            print(f"Running in PyInstaller bundle. _MEIPASS: {sys._MEIPASS}")
            return sys._MEIPASS # type: ignore
        else:
            # Development mode
            script_path = os.path.abspath(__file__)
            print(f"Running as normal script. __file__: {script_path}")
            return os.path.dirname(script_path)
    except Exception as e:
        print(f"Error in get_base_path: {e}")
        input("Error in get_base_path. Press Enter to exit...")
        sys.exit(1)

if __name__ == "__main__":
    print("--- __main__ block started ---")
    try:
        base_path = get_base_path()
        app_py_path = os.path.join(base_path, "app.py")
        print(f"Base path for app.py: {base_path}")
        print(f"Expected app.py path: {app_py_path}")

        if not os.path.exists(app_py_path):
            print(f"Error: app.py not found at: {app_py_path}")
            # This specific fallback for dev __file__ might not be robust if base_path itself is __file__'s dir
            # but get_base_path() should already handle the dev case correctly.
            # Adding a direct check for safety, though it might be redundant.
            alt_app_py_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.py")
            if os.path.abspath(__file__) != app_py_path and os.path.exists(alt_app_py_path): # Check if running script directly in source
                 print(f"Found app.py via __file__ fallback: {alt_app_py_path}")
                 app_py_path = alt_app_py_path
            else:
                input("app.py not found. Press Enter to exit...")
                sys.exit(1)
        else:
            print(f"app.py found at: {app_py_path}")

        python_executable_to_use = None
        if getattr(sys, 'frozen', False):
            # For bundled app
            pyi_python_exe_env = os.environ.get('PYI_PYTHON_EXE')
            if pyi_python_exe_env and os.path.exists(pyi_python_exe_env):
                print(f"Using PYI_PYTHON_EXE from env: {pyi_python_exe_env}")
                python_executable_to_use = pyi_python_exe_env
            else:
                if pyi_python_exe_env: # Env var was set but file doesn't exist
                    print(f"PYI_PYTHON_EXE was set to '{pyi_python_exe_env}' but file not found.")
                else: # Env var not set
                    print("PYI_PYTHON_EXE environment variable not found or invalid.")
                
                # Fallback: Try to find python.exe in _MEIPASS (less reliable but a good guess)
                if hasattr(sys, '_MEIPASS'):
                    candidate_python_exe = os.path.join(sys._MEIPASS, 'python.exe')
                    if os.path.exists(candidate_python_exe):
                        print(f"Found and using python.exe in _MEIPASS: {candidate_python_exe}")
                        python_executable_to_use = candidate_python_exe
                    else:
                        print(f"python.exe not found in _MEIPASS at {candidate_python_exe}.")
                        # As a last resort for Windows, try 'python.exe' and hope for PATH.
                        if platform.system() == "Windows":
                            print("Fallback: Will attempt to use generic 'python.exe' command (risky).")
                            python_executable_to_use = "python.exe"
                        else: # For other OS, 'python'
                            print("Fallback: Will attempt to use generic 'python' command (risky).")
                            python_executable_to_use = "python"
                else: # Frozen but no _MEIPASS (should not happen)
                     print("CRITICAL: Frozen but _MEIPASS not defined. Cannot determine bundled Python reliably.")
                     # Still set a risky fallback to allow Popen to try
                     python_executable_to_use = "python.exe" if platform.system() == "Windows" else "python"

        else:
            # For development (not frozen)
            print(f"Not frozen. Using sys.executable: {sys.executable}")
            python_executable_to_use = sys.executable

        if not python_executable_to_use:
            print("CRITICAL: Could not determine Python executable for Streamlit. Exiting.")
            input("Python executable determination failed. Press Enter to exit...")
            sys.exit(1)
        
        print(f"Final Python executable for Streamlit: {python_executable_to_use}")

        cmd = [
            python_executable_to_use,
            "-m",
            "streamlit", 
            "run", 
            app_py_path,
            "--server.headless=true", # Keep this, Streamlit handles console output
            "--server.port", "8501"
        ]

        print(f"Attempting to run command: {' '.join(cmd)}")

        use_shell = False
        # Only use shell=True if we're using a generic command like "python.exe" or "python" AND on Windows.
        # If we have a full path (from PYI_PYTHON_EXE or _MEIPASS lookup), shell=False is safer and preferred.
        if platform.system() == "Windows" and python_executable_to_use.lower() in ["python.exe", "python"]:
            print(f"Using shell=True for Popen as '{python_executable_to_use}' is a generic command on Windows.")
            use_shell = True
            
        process = subprocess.Popen(cmd, shell=use_shell) 
        print("Streamlit process launched. Waiting for it to exit...")
        process.wait()
        print(f"Streamlit process exited with code: {process.returncode}")

    except FileNotFoundError as fnf_error:
        print(f"File Not Found Error: {fnf_error}")
        print(f"This can mean that '{python_executable_to_use}' or the 'streamlit' module was not found.")
        input("Critical FileNotFoundError. Press Enter to exit...")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred in __main__: {e}")
        import traceback
        traceback.print_exc()
        input("Unexpected error occurred. Press Enter to exit...")
        sys.exit(1)
    finally:
        print("--- run_dbchat.py script finished --- ")
        # input("Script finished. Press Enter to exit...")