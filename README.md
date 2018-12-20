# ansible-module-hdfs

Ansible modules for interacting with HDFS.  Provides functionality similar to the core Ansible
[`file`](https://docs.ansible.com/ansible/file_module.html) module for the HDFS filesystem.

## Requirements
- Tested with Ansible 2.2+
- Target hosts must have the `hdfs3` python module and `libhdfs3.so` shared library installed.  See
[here](https://hdfs3.readthedocs.io/en/latest/install.html) for installation instructions.
 
## Usage example
```yaml
- hosts: hdfs-namenode
  tasks:
  
    #
    # Install requirements
    #
    
    - name: Install python-pip
      become: yes
      apt:
        name: python-pip
        state: latest
 
    - name: Install hdfs3 python HDFS client
      become: yes
      pip:
        name: hdfs3
        state: latest
 
    - name: Add libhdfs3 PPA
      become: yes
      apt_repository:
        repo: deb https://dl.bintray.com/wangzw/deb trusty contrib
        update-cache: yes
 
    - name: Install libhdfs3 dependencies
      become: yes
      apt:
        name: "{{ item }}"
        state: latest
      with_items:
        - libgsasl7
        - libntlm0
        - libprotobuf8
     
    - name: Install libhdfs3 and libhdfs3-dev
      become: yes
      apt:
        name: "{{ item }}"
        allow_unauthenticated: yes
        state: latest
      with_items:
        - libhdfs3
        - libhdfs3-dev
 
    #
    # Usage example
    #
    - name: hdfs file test
      hdfsfile:
        namenode_host: namenode.my.org
        namenode_port: 8020     # optional. default: 8020
        effective_user: user    # optional, omit to use "{{ become_user }}"
        path: /test/data
        owner: data-write
        group: data-read
        mode: 0750
        state: directory
```
