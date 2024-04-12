import os
import subprocess

def get_changed_yml_files():
    # Get list of changed files between PR branch and the base branch
    changed_files = subprocess.check_output(
        ['git', 'diff', '--name-only', 'origin/${{ github.base_ref }}', 'HEAD'],
        text=True
    )
    # Filter for only .yml files
    return [f for f in changed_files.split('\n') if f.endswith('.yml')]

def check_yml_files_for_owner(files):
    missing_owner = []
    for file in files:
        with open(file, 'r') as f:
            if 'owner:' not in f.read():
                missing_owner.append(file)
    
    return missing_owner

def main():
    yml_files = get_changed_yml_files()
    if not yml_files:
        print("No YML files changed.")
        return
    
    missing_owner = check_yml_files_for_owner(yml_files)
    if missing_owner:
        for file in missing_owner:
            print(f"::error file={file}::The YML file {file} does not contain the 'owner:' key")
        exit(1)
    else:
        print("All changed YML files contain the 'owner:' key.")

if __name__ == '__main__':
    main()
