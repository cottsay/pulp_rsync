from pulpcore.plugin import PulpPluginAppConfig


class PulpRsyncPluginAppConfig(PulpPluginAppConfig):
    """
    Entry point for pulp_rsync plugin.
    """

    name = "pulp_rsync.app"
    label = "rsync"
    version = "0.0.0"
