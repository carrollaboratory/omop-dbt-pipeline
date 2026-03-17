"""
Terra environment setup script.
Sets up github auth with ssh
Edits the global gitignore for harmonization work
Adds alias' to bash_profile for other terminal based setup commands
"""
from pathlib import Path
import argparse
import os
import shutil
import subprocess

# Environment setup
bucket = os.environ.get("WORKSPACE_BUCKET")

def get_terra_startup_paths(study_id, dbt_repo, env='jupyter'):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    env: Environment type ('jupyter' or 'rstudio') to determine home directory
    """
    # Determine home directory based on environment
    if env not in ['jupyter', 'rstudio']:
        raise ValueError(f"Invalid environment '{env}'. Must be 'jupyter' or 'rstudio'.")
    
    home_dir = Path(f'/home/{env}')
    pipeline_dir = home_dir / 'pipeline'
    repo_home_dir =  pipeline_dir / dbt_repo # user editable location for the pipeline repo
    profiles_path = repo_home_dir / 'profiles.yml'
    study_data_dir = repo_home_dir / 'data' / study_id
    validation_yml_path = repo_home_dir / 'data' / study_id / f'{study_id}_validation.yaml'
    study_yml_path = repo_home_dir / 'data' / study_id / f'{study_id}_study.yaml'
    output_dir = pipeline_dir / 'output_data'
    output_study_dir = output_dir / study_id
    output_validation_dir = output_study_dir / "validation"
    input_study_dir = pipeline_dir / '_study_data'
    sp_study_dir = input_study_dir / study_id
    seeds_dir = repo_home_dir / 'seeds'
    notebook_dir = repo_home_dir / 'notebooks'

    dbt_dir = home_dir / ".dbt" # New loc for the profiles.yml
    ssh_dir = home_dir / ".ssh"
    git_config_path = home_dir / ".gitconfig"
    id_rsa_src = home_dir / "id_rsa"
    id_rsa_dest = ssh_dir / "id_rsa"
    bash_profile = home_dir / ".bash_profile"
    terra_gitignore = home_dir / 'gitignore_global'
    bucket_study_dir = f'{bucket}/{study_id}'

    return {
        "home_dir": home_dir,
        "repo_home_dir": repo_home_dir,
        "profiles_path": profiles_path,
        "study_data_dir": study_data_dir,
        "validation_yml_path": validation_yml_path,
        "study_yml_path": study_yml_path,
        "pipeline_dir": pipeline_dir,
        "output_dir": output_dir,
        "output_study_dir": output_study_dir,
        "output_validation_dir": output_validation_dir,
        "input_study_dir": input_study_dir,
        "sp_study_dir": sp_study_dir,
        "seeds_dir": seeds_dir,
        "notebook_dir": notebook_dir,
        "dbt_dir": dbt_dir,
        "ssh_dir": ssh_dir,
        "git_config_path": git_config_path,
        "id_rsa_src": id_rsa_src,
        "id_rsa_dest": id_rsa_dest,
        "bash_profile": bash_profile,
        "terra_gitignore": terra_gitignore,
        "bucket_study_dir": bucket_study_dir,
        "bucket": bucket,
    }


# +
def setup_ssh(paths):
    # Create and configure ~/.ssh
    if not paths["ssh_dir"].is_dir():
        paths['ssh_dir'].mkdir(mode=0o700, exist_ok=True)
        print("INFO: Created ~/.ssh directory.")
    ssh_config = paths['ssh_dir'] / "config"
    if not ssh_config.exists():
        ssh_config.write_text(
            """# SSH configuration for GitHub
Host github
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_rsa
  IdentitiesOnly yes
"""
        )
        ssh_config.chmod(0o600)
        print("INFO: Created ~/.ssh/config file.")

    # Move id_rsa to ~/.ssh and set correct permissions
    if paths['id_rsa_src'].exists():
        os.system(f"mv {paths['id_rsa_src']} {paths['id_rsa_dest']}")
        paths['id_rsa_dest'].chmod(0o600)
        print(f"INFO: Moved id_rsa to {paths['id_rsa_src']} and set permissions to 600.")

    if not paths['id_rsa_src'].exists() and not paths['id_rsa_dest'].exists():
        print(f"WARNING: Make sure the private key is available.")

# See [docs](https://github.com/DataBiosphere/terra-examples/blob/main/best_practices/source_control/terra_source_control_cheatsheet.md#1-use-the-jupyter-console-to-upload-your-github-ssh-key-and-create-an-interactive-terminal-session)


# -

def update_bash_profile(paths, pipeline, umls_key, env='jupyter'):
    """Update bash_profile with aliases and env vars. Idempotent - skips if already present."""
    
    content1 = """
# Custom PS1 prompt with virtual environment display
export PS1='\\[\\033[1;33m\\]${VIRTUAL_ENV:+(venv)} \\[\\033[1;36m\\]$(basename "$PWD")\\[\\033[00m\\]\\$ '
"""

    content2 = f"""

# Add UMLS key
export UMLS_API_KEY={umls_key}
export LOCUTUS_LOGLEVEL='INFO'

# Alias to activate Python virtual environment
alias activate="source /home/{env}/venv-python3.12/bin/activate"

echo 'Alias are: activate'


"""
    
    bp = paths['bash_profile']
    
    # Check if already configured
    try:
        existing = bp.read_text()
        # Check for marker to see if already updated
        if 'LOCUTUS_LOGLEVEL' in existing and 'alias activate=' in existing:
            print("INFO: bash_profile already configured. Skipping.")
            return
    except FileNotFoundError:
        existing = ""
    
    # Append content
    with bp.open("a") as file:
        file.write("\n" + content1 + "\n" + content2)

    print("INFO: Content successfully added to ~/.bash_profile.")
    print("INFO: To apply changes, run: source ~/.bash_profile")
def stop_gitignoring_filetypes(paths):
    """Update gitignore_global to not ignore SQL and YML files. Idempotent - skips if already present."""
    content = """
!*.sql
!*.yml
"""
    
    ti = paths['terra_gitignore']
    
    # Check if already configured
    try:
        existing = ti.read_text()
        if '!*.sql' in existing and '!*.yml' in existing:
            print("INFO: gitignore_global already configured. Skipping.")
            return
    except FileNotFoundError:
        existing = ""
    
    # Append content
    with ti.open("a") as file:
        file.write("\n" + content + "\n")
    print("INFO: Content successfully added to ~/gitignore_global")


def clone_repo(paths, pipeline):
    """Clone the pipeline repo into the configured pipeline path and add PYTHONPATH export to bash_profile."""
    pipeline_dir = paths['pipeline_dir']
    repo_home = paths['repo_home_dir']
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    if not repo_home.exists():
        print(f"INFO: Cloning {pipeline} into {repo_home}")
        try:
            res = subprocess.run(["git", "clone", pipeline, str(repo_home)], check=False)
            if res.returncode == 0:
                print("INFO: Clone successful.")
            else:
                print(f"ERROR: git clone returned {res.returncode}")
        except Exception as e:
            print(f"ERROR: Exception while running git clone: {e}")
    else:
        print(f"INFO: Repo already exists at {repo_home}")

    # Ensure PYTHONPATH is added to bash_profile
    export_line = f'export PYTHONPATH="{repo_home}:$PYTHONPATH"'
    bp = paths['bash_profile']
    try:
        bp_text = bp.read_text()
    except FileNotFoundError:
        bp_text = ""
    if export_line not in bp_text:
        with bp.open("a") as f:
            f.write("\n" + export_line + "\n")
        print("INFO: Added PYTHONPATH export to bash_profile")
    else:
        print("INFO: PYTHONPATH already present in bash_profile")


def setup_data(paths):
    """Prepare data directories and copy profiles.yml into ~/.dbt"""
    for key in ("study_data_dir", "output_dir", "output_study_dir", "dbt_dir", "input_study_dir", "sp_study_dir", "output_validation_dir"):
        try:
            paths[key].mkdir(parents=True, exist_ok=True)
            print(f"INFO: Ensured directory {paths[key]}")
        except Exception as e:
            print(f"WARNING: Could not create {paths[key]}: {e}")

    # Copy profiles.yml to dbt dir if it exists
    if paths['profiles_path'].exists():
        dest = paths['dbt_dir'] / paths['profiles_path'].name
        try:
            shutil.copy2(paths['profiles_path'], dest)
            print(f"INFO: Copied {paths['profiles_path']} to {dest}")
        except Exception as e:
            print(f"WARNING: Failed to copy profiles.yml: {e}")
    else:
        print(f"WARNING: profiles.yml not found at {paths['profiles_path']} - skipping copy")


def main():
    parser = argparse.ArgumentParser(description="Setup harmonization pipeline for Terra runtime.")
    parser.add_argument(
        "-s", "--study_id", required=True, help="FTD coded study identifier for dbt. Used to create study-specific data and output dirs."
    )
    parser.add_argument("-i", "--repo_id", required=False, default='git@github.com:NIH-NCPI/anvil_dbt_project.git', help="SSH identifier for cloning.")
    parser.add_argument("-r", "--repo_dir_name", required=False, default='anvil_dbt_project', help="Name the pipeline repo to clone and create dirs for.")
    parser.add_argument("-e", "--env", required=False, default='jupyter', choices=['jupyter', 'rstudio'], help="Environment type: 'jupyter' or 'rstudio' (default: jupyter).")
    parser.add_argument("-k", "--umls_key", required=False, default="", help="The umls api key. Necessary if using search_dragon.")
    parser.add_argument("-a", "--action", required=False, 
                        choices=["update_bash_profile", "update_gitignore", "clone_pipeline", "setup_data", "setup_ssh"],
                        help="Run a single action and exit")

    args = parser.parse_args()

    paths = get_terra_startup_paths(args.study_id, args.repo_dir_name, args.env)

    # If a specific action is requested, run only that action
    if args.action:
        if args.action == "update_bash_profile":
            update_bash_profile(paths, args.repo_id, args.umls_key, args.env)
        elif args.action == "update_gitignore":
            stop_gitignoring_filetypes(paths)
        elif args.action == "clone_pipeline":
            clone_repo(paths, args.repo_id)
        elif args.action == "setup_data":
            setup_data(paths)
        elif args.action == "setup_ssh":
            setup_ssh(paths)
    else:
        # Default: run all setup actions
        update_bash_profile(paths, args.repo_id, args.umls_key, args.env)
        stop_gitignoring_filetypes(paths)
        print("INFO: Default setup complete. Use --action for individual tasks.")


if __name__ == "__main__":
    main()
