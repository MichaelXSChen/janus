
## Janus 
[![Build Status](https://travis-ci.org/NYU-NEWS/janus.svg?branch=master)](https://travis-ci.org/NYU-NEWS/janus)

Code repo for our OSDI '16 paper:
[Consolidating Concurrency Control and Consensus for Commits under Conflicts](http://mpaxos.com/pub/janus-osdi16.pdf)


## Quick start (with Ubuntu 16.04)

Install dependencies:

```
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    python-dev \
    python-pip \
    libgoogle-perftools-dev
sudo pip install -r requirements.txt
```

Get source code:
```
git clone --recursive https://github.com/NYU-NEWS/janus.git
```

Build:

```
./waf configure build -t

```
Test run:
```
./test_run.py -m janus
```

## Additional Guide by Xusheng Chen.

1. Run code with machines of the same OS version.
2. Set the coroutine Macro (comment or uncomment) in `src/rrr/reactor/coroutine.h`

    * 18.04's default boost version use coroutine2
    * 16.04's default boost version use coroutine1 

3. Setup NFS, use the same absolute path for the `$janus` directory. 

    * e.g., clone to machine1's `/home/xchen/janus`, export using NFS. Other machine mount the director to `/home/xchen/janus` too. 

4. `mkdir $janus/tmp; mkdir $janus/log; mkdir $janus/archive'

5. Setup config files in `$janus/config`. See the below wiki page (distributed experiment) for more info. 



## More
Check the [wiki page](https://github.com/NYU-NEWS/janus/wiki) to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.
<!-- 
## Do some actual good
For every star collected on this project, I will make a $25 charity loan via [Kiva] (https://www.kiva.org/invitedby/gzcdm3147?utm_campaign=permurl-share-invite-normal&utm_medium=referral&utm_content=gzcdm3147&utm_source=mpaxos.com).
-->
