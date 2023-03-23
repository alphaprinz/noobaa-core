dnf install -y epel-release || sudo dnf install -y oracle-epel-release-el$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1) || sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1).noarch.rpm
dnf install -y https://apache.jfrog.io/artifactory/arrow/almalinux/$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)/apache-arrow-release-latest.rpm
dnf config-manager --set-enabled epel || :
dnf config-manager --set-enabled powertools || :
dnf config-manager --set-enabled crb || :
dnf config-manager --set-enabled ol$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)_codeready_builder || :
dnf config-manager --set-enabled codeready-builder-for-rhel-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)-rhui-rpms || :
subscription-manager repos --enable codeready-builder-for-rhel-$(cut -d: -f5 /etc/system-release-cpe | cut -d. -f1)-$(arch)-rpms || :
dnf install -y arrow-devel # For C++
#dnf install -y arrow-glib-devel # For GLib (C)
#dnf install -y arrow-dataset-devel # For Apache Arrow Dataset C++
#dnf install -y arrow-dataset-glib-devel # For Apache Arrow Dataset GLib (C)
#dnf install -y arrow-flight-devel # For Apache Arrow Flight C++
#dnf install -y arrow-flight-glib-devel # For Apache Arrow Flight GLib (C)
#dnf install -y gandiva-devel # For Apache Gandiva C++
#dnf install -y gandiva-glib-devel # For Apache Gandiva GLib (C)
dnf install -y parquet-devel # For Apache Parquet C++
#dnf install -y parquet-glib-devel # For Apache Parquet GLib (C)
