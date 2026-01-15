# TASKS

[ ] - Implement the ABI cache at database level (which may link the contract registry to the engine db...).
[ ] - Implement the SRC5 detection.
[ ] - Implement the ABI heuristics detection.
[ ] - Finalize the ERC20 example with some nice queries + subscriptions. Working on events from torii archive will be way faster.
[ ] - Ignore reverted transactions!

# NOTES

## Speed with auto-discovery
When the auto-discovery is enabled.. torii is waaaaaaay slower. Due to the amount of contract we may not have seen.
It is speeding up afterward for sure. But there is a non-negligible delay when the auto-discovery is enabled during history syncing. Once in the head of the chain, it's not a problem at all.
