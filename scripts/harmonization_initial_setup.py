"""
Terra environment setup script.
Sets up github auth with ssh
Edits the global gitignore for harmonization work
Adds alias' to bash_profile for other terminal based setup commands
"""
from pathlib import Path
import argparse
import os

# Environment setup
bucket = os.environ.get("WORKSPACE_BUCKET")

def get_terra_startup_paths(study_id, dbt_repo):
    """
    For automatic validation of dir path creation, end the dir variables with "dir"
    """
    home_dir = Path.home()
    pipeline_dir = home_dir / 'pipeline'
    repo_home_dir =  pipeline_dir / dbt_repo # user editable location for the pipeline repo
    profiles_path = repo_home_dir / 'profiles.yml'
    study_data_dir = repo_home_dir / 'data' / study_id
    validation_yml_path = repo_home_dir / 'data' / study_id / f'{study_id}_validation.yaml'
    study_yml_path = repo_home_dir / 'data' / study_id / f'{study_id}_study.yaml'
    output_dir = pipeline_dir / 'output_data'
    output_study_dir = output_dir / study_id
    output_validation_dir = output_study_dir / "validation"
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
def setup_gh(gh_user, gh_email, paths):
    content1=f'''
[user]
        email = {gh_email}
        name = {gh_user}
[url "git@github.com:"]
        insteadOf = https://github.com/

    '''

    with paths['git_config_path'].open("a") as file:
        file.write("\n" + content1)

    print("INFO: Edited ~/.gitconfig file.")

def update_bash_profile(paths, pipeline, umls_key):

    content1 = """
# Custom PS1 prompt with virtual environment display
export PS1='\\[\\033[1;33m\\]${VIRTUAL_ENV:+(venv)} \\[\\033[1;36m\\]$(basename "$PWD")\\[\\033[00m\\]\\$ '
"""

    content2 = f"""
# Add SSH private key
eval "ssh-add ~/.ssh/id_rsa"

# Add UMLS key
eval "ssh-add ~/.ssh/id_rsa"
export UMLS_API_KEY={umls_key}

# Alias to activate Python virtual environment
alias activate="source /home/jupyter/venv-python3.12/bin/activate"

# Alias to setup ssh and permissions:
alias setup_ssh="
echo 'Assuming the id_rsa is in the {paths['id_rsa_src']} dir. Setting permissions.'
eval 'chmod 600 ~/.ssh/id_rsa'
"

# Setup dirs and clone the repo
alias clone_repo="
eval 'mkdir {paths['pipeline_dir']}'
eval 'mkdir {paths['repo_home_dir']}'
eval 'git clone {pipeline} {paths['repo_home_dir']}'
export PYTHONPATH="{paths['repo_home_dir']}:$PYTHONPATH"
"

# Alias to dbt prep file system:
alias setup_data="
eval 'mkdir {paths['study_data_dir']}'
eval 'activate'
eval 'cd {paths['repo_home_dir']}'
eval 'mkdir {paths['output_dir']}'
eval 'mkdir {paths['output_study_dir']}'
eval 'mkdir {paths['dbt_dir']}'
eval 'cp {paths['profiles_path']} {paths['dbt_dir']}'
"

# Alias to clean and compile pipeline:
alias r_dbt="
eval 'dbt clean'
eval 'dbt deps'
"

echo 'Alias are: activate, r_dbt, setup_ssh, clone_repo, setup_data'

export LOCUTUS_LOGLEVEL='INFO'

"""
    with paths['bash_profile'].open("a") as file:
        file.write("\n" + content1 + "\n" + content2)

    print("INFO: Content successfully added to ~/.bash_profile.")

    print("INFO: To apply changes, run: source ~/.bash_profile")


def stop_gitignoring_filetypes(paths):
    content = """
!*.sql
!*.yml
"""
    with paths['terra_gitignore'].open("a") as file:
        file.write("\n" + content + "\n")
    print("INFO: Content successfully added to ~/gitignore_global")


def main():
    parser = argparse.ArgumentParser(description="Get metadata for a code using the available locutus OntologyAPI connection.")

    parser.add_argument(
        "-s", "--study_id", required=True, help="Study identifier. FTD coded for dbt."
    )
    parser.add_argument("-u", "--gh_user", required=True, help="Github users username")
    parser.add_argument("-e", "--gh_email", required=True, help="Github users email")    
    parser.add_argument("-k", "--umls_key", required=False, default="", help="The umls api key. Necessary if using search_dragon.")
    parser.add_argument("-i", "--repo_id", required=False, default='git@github.com:NIH-NCPI/anvil_dbt_project.git', help="SSH version for cloning.")
    parser.add_argument("-r", "--repo", required=False, default='anvil_dbt_project', help="Name of the repo to clone and create dirs for.")
    args = parser.parse_args()

    paths = get_terra_startup_paths(args.study_id, args.repo)

    setup_ssh(paths)  # Required first time env setup
    setup_gh(args.gh_user, args.gh_email, paths)  # Required first time env setup
    update_bash_profile(paths, args.repo_id, args.umls_key)
    stop_gitignoring_filetypes(paths)


if __name__ == "__main__":
    main()
