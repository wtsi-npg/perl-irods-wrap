#!/bin/bash

set -e -u -x

WSI_NPG_GITHUB_URL=${WSI_NPG_GITHUB_URL:=https://github.com/wtsi-npg}
WSI_NPG_BUILD_BRANCH=${WSI_NPG_BUILD_BRANCH:=devel}

repos=""
for repo in perl-dnap-utilities; do
    cd /tmp

    # Clone deeper than depth 1 to get the tag even if something has been already
    # committed over the tag
    git clone --branch master --depth 3 "$WSI_NPG_GITHUB_URL/${repo}.git" "${repo}.git"
    cd "/tmp/${repo}.git"

    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin "$WSI_NPG_BUILD_BRANCH" && \
	    git pull origin "$WSI_NPG_BUILD_BRANCH" && \
	    echo "Switched to branch $WSI_NPG_BUILD_BRANCH"
    repos="$repos /tmp/${repo}.git"
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    pushd "$repo"
    cpanm --quiet --notest --installdeps .
    ./Build install
    popd
done
