use std::collections::VecDeque;

use crate::core::sm::CommandSeq;
use crate::engine::Command;
use crate::RaftTypeConfig;

/// The entry of output from Engine to the runtime.
#[derive(Debug, Default)]
pub(crate) struct EngineOutput<C>
where C: RaftTypeConfig
{
    /// A Engine level sequence number for identifying a command.
    pub(crate) seq: CommandSeq,

    /// Command queue that need to be executed by `RaftRuntime`.
    pub(crate) commands: VecDeque<Command<C>>,
}

impl<C> EngineOutput<C>
where C: RaftTypeConfig
{
    /// Generate the next command seq of an sm::Command.
    pub(crate) fn next_sm_seq(&mut self) -> CommandSeq {
        self.seq += 1;
        self.seq
    }

    /// Get the last used sm::Command seq
    pub(crate) fn last_sm_seq(&self) -> CommandSeq {
        self.seq
    }

    pub(crate) fn new(command_buffer_size: usize) -> Self {
        Self {
            seq: 0,
            commands: VecDeque::with_capacity(command_buffer_size),
        }
    }

    /// Push a command to the queue.
    pub(crate) fn push_command(&mut self, mut cmd: Command<C>) {
        tracing::debug!("push command: {:?}", cmd);

        match &mut cmd {
            Command::StateMachine { command } => {
                let seq = self.next_sm_seq();
                tracing::debug!("next_seq: {}", seq);
                command.set_seq(seq);
            }
            Command::BecomeLeader => {}
            Command::QuitLeader => {}
            Command::AppendInputEntries { .. } => {}
            Command::ReplicateCommitted { .. } => {}
            Command::Commit { .. } => {}
            Command::Replicate { .. } => {}
            Command::RebuildReplicationStreams { .. } => {}
            Command::SaveVote { .. } => {}
            Command::SendVote { .. } => {}
            Command::PurgeLog { .. } => {}
            Command::DeleteConflictLog { .. } => {}
            Command::Respond { .. } => {}
        }

        // A leader can advance the commit index several times while the runtime is
        // still draining the output queue. Applying each of those ranges separately
        // serializes a log read, state-machine callback and applied-state checkpoint
        // for every progress notification. The command queue already preserves the
        // ordering boundary we need: a leader commit is immediately preceded by the
        // `ReplicateCommitted` notification for that commit, while followers enqueue
        // Commit directly. Merge only those exact patterns and only when the next range
        // starts at the previous end. This keeps replication notifications ordered while
        // reducing apply/flush round trips.
        if let Command::Commit {
            seq,
            already_committed,
            upto,
        } = &cmd
        {
            let len = self.commands.len();
            let merge_index = if matches!(self.commands.back(), Some(Command::Commit { .. })) {
                // Followers enqueue Commit directly, without a leader-only
                // ReplicateCommitted notification.
                len.checked_sub(1)
            } else if len >= 2 && matches!(self.commands.back(), Some(Command::ReplicateCommitted { .. })) {
                // Leaders enqueue ReplicateCommitted immediately before Commit.
                len.checked_sub(2)
            } else {
                None
            };

            if let Some(merge_index) = merge_index {
                let previous = self.commands.get(merge_index);
                let merge = match previous {
                    Some(Command::Commit {
                        seq: previous_seq,
                        already_committed: previous_already,
                        upto: previous_upto,
                    }) if already_committed.as_ref() == Some(previous_upto) => {
                        Some((*previous_seq, previous_already.clone(), previous_upto.clone()))
                    }
                    _ => None,
                };

                if let Some((previous_seq, previous_already, _)) = merge {
                    tracing::debug!(previous_seq, seq, "coalescing contiguous committed apply ranges");
                    self.commands.remove(merge_index);
                    cmd = Command::Commit {
                        seq: *seq,
                        already_committed: previous_already,
                        upto: upto.clone(),
                    };
                }
            }
        }

        self.commands.push_back(cmd)
    }

    /// Put back the command to the head of the queue.
    ///
    /// This will be used when the command is not ready to be executed.
    pub(crate) fn postpone_command(&mut self, cmd: Command<C>) {
        tracing::debug!("postpone command: {:?}", cmd);
        self.commands.push_front(cmd)
    }

    /// Pop the first command to run from the queue.
    pub(crate) fn pop_command(&mut self) -> Option<Command<C>> {
        self.commands.pop_front()
    }

    /// Iterate all queued commands.
    pub(crate) fn iter_commands(&self) -> impl Iterator<Item = &Command<C>> {
        self.commands.iter()
    }

    /// Take all queued commands and clear the queue.
    #[cfg(test)]
    pub(crate) fn take_commands(&mut self) -> Vec<Command<C>> {
        self.commands.drain(..).collect()
    }

    /// Clear all queued commands.
    #[cfg(test)]
    pub(crate) fn clear_commands(&mut self) {
        self.commands.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::testing::UTConfig;
    use crate::testing::log_id;

    #[test]
    fn coalesces_only_contiguous_commit_ranges() {
        let mut output = EngineOutput::<UTConfig>::new(8);
        output.push_command(Command::ReplicateCommitted {
            committed: Some(log_id(1, 1, 1)),
        });
        output.push_command(Command::Commit {
            seq: 1,
            already_committed: None,
            upto: log_id(1, 1, 1),
        });
        output.push_command(Command::ReplicateCommitted {
            committed: Some(log_id(1, 1, 2)),
        });
        output.push_command(Command::Commit {
            seq: 2,
            already_committed: Some(log_id(1, 1, 1)),
            upto: log_id(1, 1, 2),
        });

        assert_eq!(output.take_commands(), vec![
            Command::ReplicateCommitted {
                committed: Some(log_id(1, 1, 1)),
            },
            Command::ReplicateCommitted {
                committed: Some(log_id(1, 1, 2)),
            },
            Command::Commit {
                seq: 2,
                already_committed: None,
                upto: log_id(1, 1, 2),
            },
        ]);
    }

    #[test]
    fn does_not_merge_when_a_non_contiguous_range_is_queued() {
        let mut output = EngineOutput::<UTConfig>::new(8);
        output.push_command(Command::ReplicateCommitted {
            committed: Some(log_id(1, 1, 1)),
        });
        output.push_command(Command::Commit {
            seq: 1,
            already_committed: None,
            upto: log_id(1, 1, 1),
        });
        output.push_command(Command::ReplicateCommitted {
            committed: Some(log_id(1, 1, 3)),
        });
        output.push_command(Command::Commit {
            seq: 2,
            already_committed: Some(log_id(1, 1, 2)),
            upto: log_id(1, 1, 3),
        });

        assert_eq!(output.take_commands().len(), 4);
    }

    #[test]
    fn coalesces_follower_commit_ranges_without_network_notifications() {
        let mut output = EngineOutput::<UTConfig>::new(8);
        output.push_command(Command::Commit {
            seq: 1,
            already_committed: None,
            upto: log_id(1, 1, 1),
        });
        output.push_command(Command::Commit {
            seq: 2,
            already_committed: Some(log_id(1, 1, 1)),
            upto: log_id(1, 1, 2),
        });

        assert_eq!(output.take_commands(), vec![Command::Commit {
            seq: 2,
            already_committed: None,
            upto: log_id(1, 1, 2),
        }]);
    }
}
