package edu.duke.raft;

import java.util.Timer;
import java.util.Random;

import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.net.MalformedURLException;

public class CandidateMode extends RaftMode {
    private Timer timer;

    public void go () {
        // start election:
        // 1. increment term and vote for self
        int term = mConfig.getCurrentTerm() + 1;
        mConfig.setCurrentTerm(term, mID);

        // 2. reset election timer    
        System.out.println ("S" + mID + "." + term + ": switched to candidate mode.");

        int timeout = (new Random()).nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN;
        //int timeout = 1200 / mID;
        timer = scheduleTimer(timeout, mID);

        // 3. send RequestVote RPCs to other servers
        synchronized(mLock) {
            RaftResponses.setTerm(term);
        }
        int numServers = mConfig.getNumServers();
        for (int serverID = 1; serverID <= numServers; serverID++) {
            if (mID != serverID) {
                remoteRequestVote(serverID, term, mID, mLog.getLastIndex(), mLog.getLastTerm());
            }
        }
        
        // 4. polling for vote results
        int voteCounter = 1;
        while (true) {
            int[] votes;
            synchronized(mLock) {
                votes = RaftResponses.getVotes(term);
            }
            
            for (int serverID = 1; serverID <= numServers; serverID++) {
                if (votes[serverID] == 0) {
                    voteCounter++;
                }
            }
            if (voteCounter > numServers / 2) {
                try {
                    timer.cancel();

                    String mUrl = "rmi://localhost:" + mRmiPort + "/S" + mID;
                    RaftServerImpl server = new RaftServerImpl(mID);
                    RaftServerImpl.setMode(new LeaderMode());
              
                    Naming.rebind(mUrl, server);
                } catch (MalformedURLException mue) {
                    System.out.println("S" + mID + "." + term + 
                        ": failed to switch to leader mode - invalid binding url.");
                } catch (RemoteException re) {
                    System.out.println("S" + mID + "." + term + 
                        ": failed to switch to leader mode - " + re.getMessage());   
                } finally {
                    break;
                }
            }
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
          * If RPC request contains term > currentTerm: set term = currentTerm, convert to follower
          * Otherwise do not grant vote, return server's current term
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
                mConfig.setCurrentTerm(candidateTerm, candidateID);

                String mUrl = "rmi://localhost:" + mRmiPort + "/S" + mID;
                RaftServerImpl server = new RaftServerImpl(mID);
                RaftServerImpl.setMode(new FollowerMode());

                Naming.rebind(mUrl, server);
            } catch (MalformedURLException mue) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to leader mode - invalid binding url.");
            } catch (RemoteException re) {
                System.out.println("S" + mID + "." + term + 
                    ": failed to switch to leader mode - " + re.getMessage());   
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
          * If RPC request contains term >= currentTerm: set term = currentTerm, convert to follower
          * Else if leaderTerm < currentTerm, return server's current term
          */
        int term = mConfig.getCurrentTerm();
        if (leaderTerm < term) {
            return term;
        }
        timer.cancel();
        try {
            mConfig.setCurrentTerm(leaderTerm, (leaderTerm > term) ? 0 : mConfig.getVotedFor());

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
    }

    public int clientRequest(int[] actions) {
        return -1;
    }

    // @param id of the timer that timed out
    public void handleTimeout (int timerID) {
        go();
    }
}
