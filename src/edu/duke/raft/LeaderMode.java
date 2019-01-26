package edu.duke.raft;

import java.util.Timer;
import java.util.Random;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.net.MalformedURLException;

public class LeaderMode extends RaftMode {
    private Timer timer;
    //private Timer response_timer;

    private int[] nextIndex;
    private int[] matchIndex;

    public void go () {
        int term = mConfig.getCurrentTerm();      
        System.out.println ("S" + mID + "." + term + ": switched to leader mode.");

        int numServers = mConfig.getNumServers();
        // init nextIndex to the index after last one in log
        nextIndex = new int[numServers + 1];
        matchIndex = new int[numServers + 1];
        int mylastIndex = mLog.getLastIndex();
        for (int serverID = 1; serverID <= numServers; serverID++) {
            nextIndex[serverID] = mylastIndex + 1;
            matchIndex[serverID] = 0;
        }

        // send heartbeat to other servers
        timer = scheduleTimer(HEARTBEAT_INTERVAL, mID);
        // if commitIndex > lastApplied: increment lastApplied and apply corresponding log action to state machine
        if (mCommitIndex > mLastApplied) {
            mLastApplied++;
        }
        for (int serverID = 1; serverID <= numServers; serverID++) {
            if (mID == serverID) {
                continue;
            }
            int prevLogIndex = nextIndex[serverID] - 1;
            Entry entry = mLog.getEntry(prevLogIndex);
            int prevLogTerm = entry.term;

            // number of new entries sent to follower = mylastIndex - nextIndex[serverID]
            Entry[] entries = new Entry[mylastIndex - nextIndex[serverID] + 1];
            int i = nextIndex[serverID];
            int j = 0;
            while (i <= mylastIndex) {
                entries[j] = mLog.getEntry(i);
                i++;
                j++;
            }
            remoteAppendEntries(serverID, term, mID, 
                prevLogIndex, prevLogTerm, entries, mCommitIndex);
        }

        //response_timer = scheduleTimer(HEARTBEAT_INTERVAL, -mID);
    }
  
    // @param candidate’s term
    // @param candidate requesting vote
    // @param index of candidate’s last log entry
    // @param term of candidate’s last log entry
    // @return 0, if server votes for candidate; otherwise, server's
    // current term
    public int requestVote (int candidateTerm, int candidateID, int lastLogIndex, int lastLogTerm) {
        /**
          * If candidateTerm > currentTerm: grant vote for candidate, convert to follower
          * else return server's current term
          */
        int term = mConfig.getCurrentTerm();
        if (candidateTerm < term) {
            return term;
        }
        int mylastIndex = mLog.getLastIndex();
        int mylastTerm = mLog.getEntry(mylastIndex).term;
        int votedFor = mConfig.getVotedFor();
        if ((candidateTerm > term || (votedFor == 0 || votedFor == candidateID)) 
            && (lastLogTerm > mylastTerm || (lastLogTerm == mylastTerm && lastLogIndex >= mylastIndex))) {
            try {
                timer.cancel();
                //response_timer.cancel();
                mConfig.setCurrentTerm(candidateTerm, candidateID);

                String mUrl = "rmi://localhost:" + mRmiPort + "/S" + mID;
                RaftServerImpl server = new RaftServerImpl(mID);
                RaftServerImpl.setMode(new FollowerMode());

                Naming.rebind(mUrl, server);
            } catch (MalformedURLException mue) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to follower mode - invalid binding url.");
            } catch (RemoteException re) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to follower mode - " + re.getMessage());
            } finally {
                return 0;
            }
        } else {
            return term;
        }
    }
  

    // @param leader’s term
    // @param current leader
    // @param index of log entry before entries to append
    // @param term of log entry before entries to append
    // @param entries to append (in order of 0 to append.length-1)
    // @param index of highest committed entry
    // @return 0, if server appended entries; otherwise, server's
    // current term
    public int appendEntries (int leaderTerm, int leaderID, int prevLogIndex, int prevLogTerm, Entry[] entries, int leaderCommit) {
        /**
          * If RPC request contains term > currentTerm: set term = currentTerm, convert to follower
          * else return server's current term
          */
        int term = mConfig.getCurrentTerm();
        if (leaderTerm > term) {
            try {
                timer.cancel();
                //response_timer.cancel();
                mConfig.setCurrentTerm(leaderTerm, 0);

                String mUrl = "rmi://localhost:" + mRmiPort + "/S" + mID;
                RaftServerImpl server = new RaftServerImpl(mID);
                RaftServerImpl.setMode(new FollowerMode());

                Naming.rebind(mUrl, server);
            } catch (MalformedURLException mue) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to follower mode - invalid binding url.");
            } catch (RemoteException re) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to follower mode - " + re.getMessage());
            } finally {
                return 0;
            }
        } else {
            return term;
        }

    }

    public int clientRequest(int[] actions) {
        int term = mConfig.getCurrentTerm();
        Entry[] entries = new Entry[actions.length];
        for (int i=0; i<actions.length; i++) {
            entries[i] = new Entry(actions[i], term);
        }
        mLog.insert(entries, mLog.getLastIndex(), mLog.getLastTerm());
        return 0;
    }

    // @param id of the timer that timed out
    public void handleTimeout (int timerID) {
        // check last heartbeat response
        int term = mConfig.getCurrentTerm();
        int numServers = mConfig.getNumServers();
        int[] responses;
        synchronized(mLock) {
            RaftResponses.setTerm(term);
            responses = RaftResponses.getAppendResponses(term);
        }
        for (int serverID = 1; serverID <= numServers; serverID++) {
            if (serverID == mID) {
                continue;
            }
            // if appendRequest fails, decrement nextIndex by 1, and retry in next heartbeat
            if (responses[serverID] != 0) {
                if (nextIndex[serverID] != 0) {
                    nextIndex[serverID] = nextIndex[serverID] - 1;
                }
            } 
            // if appendRequest succeeds, update nextIndex and matchIndex
            else {
                nextIndex[serverID] = mLog.getLastIndex()+1;
                matchIndex[serverID] = mLog.getLastIndex(); 
            }
        }
        
        // repetitively send heartbeat
        timer = scheduleTimer(HEARTBEAT_INTERVAL, mID);
        int mylastIndex = mLog.getLastIndex();

        for (int serverID = 1; serverID <= numServers; serverID++) {
            if (mID == serverID) {
                continue;
            }
            int prevLogIndex = nextIndex[serverID] - 1;
            Entry entry = mLog.getEntry(prevLogIndex);
            int prevLogTerm = entry.term;

            // number of new entries sent to follower = mylastIndex - nextIndex[serverID]
            Entry[] entries = new Entry[mylastIndex - nextIndex[serverID] + 1];
            int i = nextIndex[serverID];
            int j = 0;
            while (i <= mylastIndex) {
                entries[j] = mLog.getEntry(i);
                i++;
                j++;
            }
            remoteAppendEntries(serverID, term, mID, 
                prevLogIndex, prevLogTerm, entries, mCommitIndex);
        }
        
        // If there exists an N such that N > commitIndex, 
        // a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
        int N = mLog.getLastIndex();
        int counter = 1;
        while (N > mCommitIndex) {
            for (int serverID = 1; serverID <= numServers; serverID++) {
                if (matchIndex[serverID] >= N) {
                    counter++;
                }
            }
            if (counter > numServers / 2 + 1 && mLog.getEntry(N).term == term) {
                mCommitIndex = N;
                break;
            }
            N--;
        }
        // if commitIndex > lastApplied: increment lastApplied and apply corresponding log action to state machine
        if (mCommitIndex > mLastApplied) {
            mLastApplied++;
        }
    }
}
