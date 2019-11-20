import subprocess


# From http://stackoverflow.com/questions/12826723/possible-to-extract-the-git-repo-revision-hash-via-python-code
def get_git_hash():
    git_process = subprocess.Popen(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (git_process_out, _) = git_process.communicate()
    return git_process_out.strip().decode()
