import posixpath

from starcluster import clustersetup, exception
from starcluster.utils import print_timing
from starcluster.logger import log


class NFSSharePlugin(clustersetup.DefaultClusterSetup):
    """
    Allows sharing of path on master node with worker nodes.  Example use case would be
    to allow scratch space on master to be shared with all worker nodes.

    Typical config file  settings may look like:

    [plugin nfsshare]
    SETUP_CLASS = nfsshare.NFSSharePlugin
    SERVER_PATH = /mnt
    CLIENT_PATH = /share
    EXPORT_NFS_SETTINGS = sync,no_root_squash,no_subtree_check,rw
    MOUNT_NFS_SETTINGS = vers=3,user,rw,exec,noauto
    """

    def __init__(self, server_path, client_path=None, export_nfs_settings=None, mount_nfs_settings=None, link_on_master=True, **kwargs):
        """
        @param server_path: path on master node to be shared over NFS
        @param client_path: path on node to mount NFS share (defaults to path on the server)
        @param export_nfs_settings: settings for exporting path on NFS server, e.g. '(sync,no_root_squash,no_subtree_check,rw)'
        @param mount_nfs_settings: settings for mounting path on NFS client, e.g. 'vers=3,user,rw,exec,noauto'
        @param link_on_master: if true and if server_path and client_path are different then create symbolic link to client_path on server
        @param kwargs: catch-all for unhandled plugin options
        @return:
        """

        client_path = client_path or server_path

        # TODO: I would like to support possibility to sharing multiple paths (how to do this in StarCluster config?)
        self.nfs_mounts = dict(zip([server_path],[client_path]))
        self.nfs_export_settings = export_nfs_settings
        self.nfs_mount_settings = mount_nfs_settings
        self.link_on_master = link_on_master

        super(NFSSharePlugin, self).__init__(**kwargs)

    def _mount_nfs_shares_on_node(self, node, server_node, nfs_mounts, nfs_mount_settings=None):
        """
        Mount each path in remote_paths from the remote server_node

        server_node - remote server node that is sharing the remote_paths
        remote_paths - list of remote paths to mount from server_node

        @param node: the node object for which path is being mounted
        @param server_node: the master node object
        @param nfs_mounts: the nfs mount points {server_path: client_path}
        @param nfs_mount_settings: optional nfs mount settings
        @return:
        """

        node.ssh.execute('/etc/init.d/portmap start')
        # TODO: move this fix for xterm somewhere else
        node.ssh.execute('mount -t devpts none /dev/pts',
                         ignore_exit_status=True)
        mount_map = node.get_mount_map()
        mount_paths = []
        for path in nfs_mounts.keys():
            network_device = "%s:%s" % (server_node.alias, path)
            if network_device in mount_map:
                mount_path, typ, options = mount_map.get(network_device)
                log.debug('nfs share %s already mounted to %s on '
                          'node %s, skipping...' %
                          (network_device, mount_path, node.alias))
            else:
                mount_paths.append(path)

        server_paths = map(lambda x: '%s:%s' % (server_node.alias,x), mount_paths)
        server_paths_regex = '|'.join(map(lambda x: x.center(len(x) + 2),
                                          server_paths))
        node.ssh.remove_lines_from_file('/etc/fstab', server_paths_regex)
        fstab = node.ssh.remote_file('/etc/fstab', 'a')

        nfs_mount_settings = nfs_mount_settings or "vers=3,user,rw,exec,noauto"

        for path in mount_paths:
            fstab.write('%s:%s %s nfs %s 0 0\n' %
                            (server_node.alias, path, nfs_mounts[path], nfs_mount_settings))

        fstab.close()
        for path in mount_paths:
            remote_path = nfs_mounts[path]
            if not node.ssh.path_exists(remote_path):
                node.ssh.makedirs(remote_path)
            node.ssh.execute('mount %s' % remote_path)

    def _mount_nfs_shares(self, nodes, mount_map, nfs_mount_settings=None):
        """
        Setup /etc/fstab and mount each nfs share listed in export_paths on
        each node in nodes list (optionally at the specified mount point)

        @param nodes: the nodes to mount nfs exports
        @param mount_map: maps nfs exports on the server to mount points on the client
        @param nfs_mount_settings: optional nfs mount settings
        @return:
        """

        log.info("Mounting all NFS export path(s) on %d worker node(s)" %
                 len(nodes))

        for node in nodes:
            self.pool.simple_job(self._mount_nfs_shares_on_node,
                                 (node, self._master, mount_map, nfs_mount_settings),
                                 jobid=node.alias)
        self.pool.wait(numtasks=len(nodes))

    def _export_fs_to_nodes(self, master, nodes, export_paths, nfs_export_settings=None):
        """
        Export each path in export_paths to each node in nodes via NFS

        nodes - list of nodes to export each path to
        export_paths - list of paths on this remote host to export to each node

        Example:
        # export /home and /opt/sge6 to each node in nodes
        $ node.start_nfs_server()
        $ node.export_fs_to_nodes(nodes=[node1,node2],
                                  export_paths=['/home', '/opt/sge6'])
        """

        # setup /etc/exports
        log.info("Configuring NFS exports path(s):\n%s" %
                 ' '.join(export_paths))
        nfs_export_settings = "(%s)" % nfs_export_settings or "(async,no_root_squash,no_subtree_check,rw)"
        etc_exports = master.ssh.remote_file('/etc/exports', 'r')
        contents = etc_exports.read()
        etc_exports.close()
        etc_exports = master.ssh.remote_file('/etc/exports', 'a')
        for node in nodes:
            for path in export_paths:
                export_line = ' '.join(
                    [path, node.alias + nfs_export_settings + '\n'])
                if export_line not in contents:
                    etc_exports.write(export_line)
        etc_exports.close()

        # TODO: does this disrupt existing client connections?
        master.ssh.execute('exportfs -fra')


    @print_timing("Setting up NFS")
    def _setup_nfs(self, nfs_mounts, nodes=None, start_server=True, nfs_export_settings=None, nfs_mount_settings=None, link_on_master=True):
        """
        Setup NFS to shares.  Differs from the StarCluster implementation in that it allows destination mount path
         to be optionally specified.

        @param nodes: where to export / mount the NFS shares
        @param start_server: boolean determines if NFS server must be started
        @param export_paths: paths on master node to be exported
        @param mount_paths: paths on destination to be mounted
        @return:
        """

        master = self._master
        # setup /etc/exports and start nfsd on master node
        nodes = nodes or self.nodes

        # this will ensure that remote_path is also replicated on the master node
        # in case that remote and server paths are different
        if link_on_master:
            for path in nfs_mounts.keys():
                if path != nfs_mounts[path] and not master.ssh.path_exists(nfs_mounts[path]):
                    master.ssh.execute('ln -s %s %s' % (path, nfs_mounts[path]))

        if start_server:
            master.start_nfs_server()

        if nodes:
            self._export_fs_to_nodes(master, nodes, export_paths=nfs_mounts.keys(),nfs_export_settings=nfs_export_settings)
            self._mount_nfs_shares(nodes, mount_map=nfs_mounts, nfs_mount_settings=nfs_mount_settings)

    def _unmount_shares(self, node, remote_paths=None):
        """
        Unmounts the mounted paths on this drive.  Can be useful when calling 'starcluster removenode -k' followed
        by 'starcluster addnode -x'.

        @param node: the node on which the paths are to be unmounted
        @return:
        """

        remote_paths = remote_paths or self.nfs_mounts.values()

        for path in remote_paths:
            cmd = "umount -fl %s" % path
            node.ssh.execute(cmd)

    def run(self, nodes, master, user, user_shell, volumes):
        log.info("NFSShare...")
        try:
            self._nodes = nodes
            self._master = master
            self._user = user
            self._user_shell = user_shell
            self._volumes = volumes

            self._setup_nfs(nfs_mounts=self.nfs_mounts, nodes=self.nodes, start_server=False,
                            nfs_export_settings=self.nfs_export_settings, nfs_mount_settings=self.nfs_mount_settings,
                            link_on_master=self.link_on_master)
        finally:
            self.pool.shutdown()

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        try:
            self._nodes = nodes
            self._master = master
            self._user = user
            self._user_shell = user_shell
            self._volumes = volumes
            log.info("Adding %s to NFSShare" % node.alias)

            self._setup_nfs(nfs_mounts=self.nfs_mounts, nodes=[node], start_server=False,
                            nfs_export_settings=self.nfs_export_settings, nfs_mount_settings=self.nfs_mount_settings)
        finally:
            self.pool.shutdown()

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        try:
            self._nodes = nodes
            self._master = master
            self._user = user
            self._user_shell = user_shell
            self._volumes = volumes
            log.info("Removing %s from NFSShare" % node.alias)

            self._remove_nfs_exports(node)
            self._unmount_shares(node)
        finally:
            self.pool.shutdown()