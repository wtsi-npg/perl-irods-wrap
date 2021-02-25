#!/bin/bash

set -e -u -x

# The default build branch for all repositories. This defaults to
# TRAVIS_BRANCH unless set in the Travis build environment.
WSI_NPG_BUILD_BRANCH=${WSI_NPG_BUILD_BRANCH:=$TRAVIS_BRANCH}

sudo apt-get install -qq uuid-dev

wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.6.14-Linux-x86_64.sh -O ~/miniconda.sh

/bin/bash ~/miniconda.sh -b -p ~/miniconda
~/miniconda/bin/conda clean -tipsy
echo ". ~/miniconda/etc/profile.d/conda.sh" >> ~/.bashrc
echo "conda activate base" >> ~/.bashrc

. ~/miniconda/etc/profile.d/conda.sh
conda activate base
conda config --set auto_update_conda False
conda config --prepend channels "$WSI_CONDA_CHANNEL"
conda config --append channels conda-forge

conda create -y -n travis
conda activate travis
conda install -y baton"$BATON_VERSION"
conda install -y irods-icommands"$IRODS_VERSION"

mkdir -p ~/.irods

cat <<'EOF' > ~/.irods/irods_environment.json
{
    "irods_host": "localhost",
    "irods_port": 1247,
    "irods_user_name": "irods",
    "irods_zone_name": "testZone",
    "irods_home": "/testZone/home/irods",
    "irods_default_resource": "replResc"
}
EOF

cpanm --local-lib=~/perl5 local::lib && eval "$(perl -I ~/perl5/lib/perl5/ -Mlocal::lib)"

# WTSI NPG Perl repo dependencies, only one at the moment
repos=""
for repo in perl-dnap-utilities; do
    cd /tmp
    # Always clone master when using depth 1 to get current tag
    git clone --branch master --depth 1 "$WSI_NPG_GITHUB_URL/${repo}.git" "${repo}.git"
    cd "/tmp/${repo}.git"
    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin "$WSI_NPG_BUILD_BRANCH" && git pull origin "$WSI_NPG_BUILD_BRANCH" && echo "Switched to branch $WSI_NPG_BUILD_BRANCH"
    repos="$repos /tmp/${repo}.git"
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd "$repo"
    cpanm --quiet --notest --installdeps .
    ./Build install
done

cd "$TRAVIS_BUILD_DIR"

cpanm --quiet --notest --installdeps .
