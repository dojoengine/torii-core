#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InstalledTokenSupport {
    pub erc20: bool,
    pub erc721: bool,
    pub erc1155: bool,
}

impl InstalledTokenSupport {
    pub const fn any(self) -> bool {
        self.erc20 || self.erc721 || self.erc1155
    }
}

pub const fn resolve_installed_token_support(
    index_external_contracts: bool,
    explicit_targets: InstalledTokenSupport,
) -> InstalledTokenSupport {
    InstalledTokenSupport {
        erc20: index_external_contracts || explicit_targets.erc20,
        erc721: index_external_contracts || explicit_targets.erc721,
        erc1155: index_external_contracts || explicit_targets.erc1155,
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve_installed_token_support, InstalledTokenSupport};

    #[test]
    fn installs_all_token_support_when_external_indexing_is_enabled() {
        let support = resolve_installed_token_support(true, InstalledTokenSupport::default());
        assert_eq!(
            support,
            InstalledTokenSupport {
                erc20: true,
                erc721: true,
                erc1155: true,
            }
        );
        assert!(support.any());
    }

    #[test]
    fn installs_no_token_support_without_targets_or_external_indexing() {
        let support = resolve_installed_token_support(false, InstalledTokenSupport::default());
        assert_eq!(support, InstalledTokenSupport::default());
        assert!(!support.any());
    }

    #[test]
    fn installs_only_explicit_target_types_when_external_indexing_is_disabled() {
        let support = resolve_installed_token_support(
            false,
            InstalledTokenSupport {
                erc20: true,
                erc721: false,
                erc1155: true,
            },
        );
        assert_eq!(
            support,
            InstalledTokenSupport {
                erc20: true,
                erc721: false,
                erc1155: true,
            }
        );
        assert!(support.any());
    }
}
