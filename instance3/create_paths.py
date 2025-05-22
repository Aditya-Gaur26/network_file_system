import os
import shutil

# List of paths to create (without 'myserver/' prefix)
PATHS_TO_CREATE = [
    'dir_vhfih/dir_lhkzx/dir_ctpcj/',
    'dir_vhfih/dir_lhkzx/dir_ctpcj/dir_apsck/',
    'dir_vhfih/dir_lhkzx/dir_ctpcj/dir_apsck/file_hwa.txt',
    'dir_vhfih/dir_lhkzx/dir_ctpcj/dir_apsck/file_rkn.txt',
    'dir_vhfih/dir_lhkzx/dir_ctpcj/dir_hfccx/',
    'dir_vhfih/dir_lhkzx/dir_ctpcj/dir_hfccx/here.txt'
]

def clear_directory(directory):
    """
    Clears the contents of the specified directory if it exists.
    
    Args:
        directory (str): The path of the directory to clear.
    """
    if os.path.exists(directory):
        shutil.rmtree(directory)
        print(f"Cleared directory: {directory}")

def create_paths(paths):
    """
    Create files and directories based on the given paths.
    Creates paths in the 'myserver' folder in the current script's directory.
    
    Args:
        paths (list): List of file or directory paths to create.
    """
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create myserver folder path
    myserver_dir = os.path.join(script_dir, 'myserver')
    
    # Clear the myserver directory if it exists
    clear_directory(myserver_dir)
    
    # Create myserver directory
    os.makedirs(myserver_dir, exist_ok=True)
    
    for path in paths:
        # Join myserver directory with the path
        full_path = os.path.join(myserver_dir, path)
        
        # Expand user home directory and get absolute path
        full_path = os.path.abspath(os.path.expanduser(full_path))
        
        try:
            # If path ends with a directory separator, it's a directory
            if path.endswith(os.path.sep):
                os.makedirs(full_path, exist_ok=True)
                print(f"Created directory: {full_path}")
            else:
                # Ensure the directory exists
                os.makedirs(os.path.dirname(full_path), exist_ok=True)
                
                # Create an empty file
                open(full_path, 'a').close()
                print(f"Created file: {full_path}")
        
        except Exception as e:
            print(f"Error creating {full_path}: {e}")

def main():
    try:
        create_paths(PATHS_TO_CREATE)
        print("Path creation completed successfully.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
