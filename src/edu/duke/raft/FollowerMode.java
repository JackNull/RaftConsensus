package edu.duke.raft;

import java.util.Timer;
import java.util.Random;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.net.MalformedURLException;

public class FollowerMode extends RaftMode {
    private Timer timer;

    public void go () {
        int term = mConfig.getCurrentTerm();
        System.out.println ("S" + mID + "." + term + ": switched to follower mode.");
        
        //int timeout = (new Random()).nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN; 
        int timeout = 1200 / mID;
        timer = scheduleTimer(timeout, mID);
        // if commitIndex > lastApplied: increment lastApplied and apply corresponding log action to state machine
        if (mCommitIndex > mLastApplied) {
            mLastApplied++;
        }
    }
  
    // @param candidate’s term
    // @param candidate requesting vote
    // @param index of candidate’s last log entry
    // @param term of candidate’s last log entry
    // @return 0, if server votes for candidate; otherwise, server's
    // current term
    public int requestVote (int candidateTerm, int candidateID, int lastLogIndex, int lastLogTerm) {
        /**
          * If candidateTerm > currentTerm: grant vote for candidate, set currentTerm = candidateTerm
          * Else return currentTerm
          */
        // If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date. If the logs
        // end with the same term, then whichever log is longer is
        // more up-to-date
        int term = mConfig.getCurrentTerm();
        if (candidateTerm < term) {
            return term;
        }
        int mylastIndex = mLog.getLastIndex();
        int mylastTerm = mLog.getEntry(mylastIndex).term; 
        int votedFor = mConfig.getVotedFor();
        if ((candidateTerm > term || (votedFor == 0 || votedFor == candidateID)) 
            && (lastLogTerm > mylastTerm || (lastLogTerm == mylastTerm && lastLogIndex >= mylastIndex))) {
            timer.cancel();
            
            mConfig.setCurrentTerm(candidateTerm, candidateID);
            int timeout = (new Random()).nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN;
            //int timeout = 1200 / mID; 
            timer = scheduleTimer(timeout, mID);

            return 0;
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
        // 1. Reply false if leaderTerm < currentTerm
        int term = mConfig.getCurrentTerm();
        if (leaderTerm < term) {
            return term;
        }
        timer.cancel();
        if (leaderTerm > term) {
            mConfig.setCurrentTerm(leaderTerm, 0);
        }
        int timeout = (new Random()).nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN;
        //int timeout = 1200 / mID;
        timer = scheduleTimer(timeout, mID);
        // if commitIndex > lastApplied: increment lastApplied and apply corresponding log action to state machine
        if (mCommitIndex > mLastApplied) {
            mLastApplied++;
        }

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        Entry entry = mLog.getEntry(prevLogIndex);
        if (prevLogIndex != -1 && (entry == null || entry.term != prevLogTerm)) {
            return term;
        } else {
            // 3. log match found, delete all following conflicting entries and append new ones 
            int mylastIndex = mLog.getLastIndex();
            if (mylastIndex > prevLogIndex) {
                mLog.delete(prevLogIndex);
            }
            if (entries.length != 0) {
                mLog.insert(entries, prevLogIndex, prevLogTerm);
            }
            // 4. set commitIndex = min(leaderCommit, index of last new entry)
            if (leaderCommit > mCommitIndex) {
                if (entries.length == 0 || leaderCommit < prevLogIndex + entries.length) {
                    mCommitIndex = leaderCommit;
                } else {
                    mCommitIndex = prevLogIndex + entries.length;
                } 
            }
            return 0;
        }
    }  

    public int clientRequest(int[] actions) {
        return -1;
    }

    // @param id of the timer that timed out
    public void handleTimeout (int timerID) {
        // start leader election
        int term = mConfig.getCurrentTerm();
        try {
            // switch to candidate mode
            String mUrl = "rmi://localhost:" + mRmiPort + "/S" + mID;
            RaftServerImpl server = new RaftServerImpl(mID);
            RaftServerImpl.setMode(new CandidateMode());
              
            Naming.rebind(mUrl, server);
        } catch (MalformedURLException mue) {
            System.out.println("S" + mID + "." + term + 
                ": failed to switch to candidate mode - invalid binding url.");
        } catch (RemoteException re) {
            System.out.println("S" + mID + "." + term + 
                ": failed to switch to candidate mode - " + re.getMessage());   
        }
    }
}

