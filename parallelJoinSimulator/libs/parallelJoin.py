import numpy as np 
import sys
import time
from collections import deque
from libs.utils import positivePart, findMinimumExcluding, lcmm, findMaximumExcluding
from fractions import Fraction
from decimal import Decimal

class ProcessingUnits:
    """ Simlation of the processing units
        for a data streaming process with
        time-based window
    """
    def __init__(self, windowType, wR = 2, wS = 2, dt = 1, num_r = 1, num_s = 1, n_init = 1, w_max = 100, determinism=True, quota = 1.0):

        # Parameters of the Processing unit
        self.wR    = wR;     # Window size for stream R
        self.wS    = wS;     # Window size for stream S
        self.dt    = dt;     # Time interval [s]
        self.w_max = w_max;  # Maximum length of the window
        self.determinism = determinism # Determinism
        self.windowType = windowType   # Window Type (0 = time based, 1 = tuple based)

        # Variables of the Processing units
        self.alpha   = 1;      # Time to perform a comparison [s/comparison]
        self.beta    = 1;      # Time to output a tuple [s/tuple]
        self.sigma   = 1;      # Selectivity [tuples/comparison]
        self.apsb    = self.alpha + self.sigma * self.beta # Auxiliary variable [s/comparison]
        self.n       = n_init; # Initial number of processing units
        self.quota   = quota;  # Quota of time for overloaded case

        self.r       = 0.0;    # Arrival rate of stream R
        self.s       = 0.0;    # Arrival rate of stream S
        self.r_sum   = 0.0;    # Sum of all the physical streams R
        self.s_sum   = 0.0;    # Sum of all the physical streams S

        # Make a list of the past history
        if self.windowType == 0:    # Time-based

            self.r_vec   = deque([[0.0]*num_r] * self.wR, maxlen = w_max);
            self.s_vec   = deque([[0.0]*num_s] * self.wS, maxlen = w_max);
        
        else:    # tuple-based
            self.r_vec   = deque([0.0]);
            self.s_vec   = deque([0.0]);

        self.curr_time = 0;        # Current time interval
        self.queue_ks  = deque([]) # Queue of non-processed requests
                                   # 0: time
                                   # 1: residual
                                   # 2: requested time k
                                   # 3: latency

        # Initialize the variables
        self.omegaR  = 0;        # Tuples to be compared from streaming R [tuples]
        self.omegaS  = 0;        # Tuples to be compared from streaming S [tuples]

        self.C       = 0;        # Comparisons to be performed [comparison]

        self.k       = 0;        # Time to process all the comparisons

        self.y       = 0;        # Number of comparisons that are output [comparison]

        self.latency = 0;        # Overall latency

        self.latency_overload = 0; # Latency corrected in case of overload
    # End initialization


    # Append newElement in a buffer
    def updateVec(self, vec, newElement):
        vec.pop();                  # Removing last element in the buffer
        vec.appendleft(newElement); # Adding new element in the buffer's head
        return vec;

    # Compute the total number of tuples to be compared and updates the arrival rates of the two streamings
    def updateTuples(self, r, s):
        self.r = r;
        self.s = s;
        self.r_sum = sum(r)
        self.s_sum = sum(s)

        if self.windowType == 0: # window type = time-based
            # Update the buffer of the incoming requests
            self.r_vec   = self.updateVec(self.r_vec, self.r);    # Adding arrival rate r
            self.s_vec   = self.updateVec(self.s_vec, self.s);    # Adding arrival rate s

            # 
            self.omegaR = sum(sum(self.r_vec,[]));
            assert self.omegaR >= 0;

            self.omegaS = sum(sum(self.s_vec,[]));
            assert self.omegaS >= 0;
        else: # window type = tuple-based
            self.omegaR = min(self.omegaR + self.r_sum, self.wR)
            self.omegaS = min(self.omegaS + self.s_sum, self.wS)

    # Compute the number of comparisons to be performed per processing units C
    def updateComparisons(self):
        # Evenly distribute the load among the self.n processing units
        omegaR_per_unit = self.omegaR / self.n;
        omegaS_per_unit = self.omegaS / self.n;

        self.C = (omegaS_per_unit  * self.r_sum + omegaR_per_unit * self.s_sum) * self.dt;
        
        assert self.C >= 0; # Ensure that the total number of comparison is always non-negative

    # Compute the time to process the current comparisons k
    def updateCost(self):
        self.k = self.C * self.apsb;
        
        assert self.k >= 0; # Ensure that the compute time for the current amount of comparisons is non-negative

    # Compute the number of comparisons that are output y
    def updateComparisonOutput(self):

        self.y = min( self.dt/self.apsb, self.C );
        
        assert self.y >= 0;    # Ensure that the number of comparison is always non-negative

    def getLatency(self,rates):
        delays = []
        for r in range(0, len(rates)):
            this_rate = rates[r]
            this_tuples = np.arange(0, 1, 1 / this_rate)
            for t in this_tuples:
                mins = []
                for s in range(0, len(rates)):
                    if s != r:
                        other_rate = rates[s]
                        if s > r:
                            mins.append([i for i in np.arange(0, 2, 1 / other_rate) if i >= t][0])
                        else:
                            mins.append([i for i in np.arange(0, 2, 1 / other_rate) if i > t][0])
                delays.append(max(mins)-t)
        return np.mean(delays)

    # Compute the latency
    def updateLatency(self):

        all_rates   = self.r + self.s;    # Concatenate all the tuples
        L           = len(all_rates);     # Length of all the tuples
        all_periods = [ Fraction(Decimal(1)/Decimal(all_rates[i])) if Decimal(all_rates[i])> 0 else 0.0 for i in range(0,L) ];    # Compute all the periods
        all_epsilons = [ 1e-10*i for i in range(0,L) ];    # All the epsilons for all the input rates
        
        all_epsilons_out = [ 1e-10*i for i in range(0,self.n) ];    # All the epsilons for all the processing units
        
        HP          = min(lcmm(*all_periods),self.dt)  # Computation of the Hyper-Period

        # Computing term related to the join (queueing requests, In model A in paper)
        if self.sigma > 0 and self.r_sum + self.s_sum > 0:
            ell_join_r = (self.sigma * self.omegaR/self.n + 1) * self.apsb  / (2 * self.sigma);
            ell_join_s = (self.sigma * self.omegaS/self.n + 1) * self.apsb  / (2 * self.sigma);
            ell_join   = self.r_sum / ( self.r_sum + self.s_sum ) * ell_join_s + \
                         self.s_sum / ( self.r_sum + self.s_sum ) * ell_join_r;
        else:
            ell_join = 0.0;

        # Computing term related to the (In model B in the paper)
        if self.determinism:
            ell = 0.0                           # Sum of all the latencies
            ess = float(HP * sum(all_rates))    # Sum of all the rates
            if ess > 0:
                for j in range(0,L):
                    # Compute the other part of the sum
                    for m in range( 0, int(HP*all_rates[j]) ):

                        mm = [float(px) * np.ceil( (m * all_periods[j] + all_epsilons[j]) /float(px) ) + ex  for px, ex in zip(all_periods,all_epsilons) ];
                        ell = ell + findMaximumExcluding(mm,j) - (m * float(all_periods[j]) + all_epsilons[j] );

                ell_readyIn = ell/ess;

            else:
                ell_readyIn = 0.0;

        else:
            ell_readyIn = 0.0;


        # Computing term related to the parallelism
        if self.determinism and self.n > 1:
            # Compute the current output_rate
            output_rate = min(self.y * self.sigma, self.r_sum + self.s_sum);

            if output_rate > 0:
                # Compute the output period
                output_period = 1.0/output_rate;
                ell_readyOut = 0.0;

                # Compute the average discrepancy for the processing units output tuples
                for j in range(0,self.n):
                    ej = all_epsilons_out[j];
                    vec_out = [output_period * np.ceil(ej/output_period) + all_epsilons_out[x] - ej for x in range(0,self.n) if x!=j];
                    ell_readyOut += max(vec_out)

                ell_parallelism = ell_readyOut/self.n; 
            else:
                ell_parallelism = 0.0;
        else:
            ell_parallelism = 0.0


        # Composing the whole latency
        self.latency = ell_join + ell_readyIn + ell_parallelism;

        assert self.latency >= 0; # Ensure that the computed latency is always non-negative
        return ell_readyIn,ell_join,ell_parallelism

    # Enqueue residual computation time that cannot be computed in the current time window
    def updateQueueKs(self):
        # Append the current computation            # Queue of non-processed requests
        self.queue_ks.append([self.curr_time,       # 0: time
                              self.k,               # 1: residual
                              self.k,               # 2: requested time k
                              self.latency,         # 3: latency
                              self.apsb])           # 4: current alpha + sigma * beta

        # Current budget for computation
        residual_budget = self.dt * self.quota;    # Quota of time that is available for processing
        total_used_budget = 0.0;
        total_comparisons = 0.0;
        served = deque([]);

        # If there is still time, and there are still comparisons to process
        while residual_budget > 0.0 and len(self.queue_ks) > 0:
            # Store current budget
            curr_budget = residual_budget;

            residual_budget = positivePart(residual_budget - self.queue_ks[0][1]);    # Remove from the budget the time requested
            used_budget     = curr_budget - residual_budget;                          # Used budget to serve the current request
            self.queue_ks[0][1] = self.queue_ks[0][1] - used_budget;                  # Remove from the requested time the available budget
            # Fix numerical issue
            if self.queue_ks[0][1] < 1e-6:
                self.queue_ks[0][1] = 0.0

            # Store the information of the served requests in a queue
            delay = (self.curr_time - self.queue_ks[0][0]) * self.dt

            # Served tuples:
            outputComparisons = used_budget / self.queue_ks[0][4];
            served.append([used_budget,             # 0: budget needed to serve the requests
                           delay,                   # 1: delay in serving the request
                           self.queue_ks[0][3],     # 2: latency for the requests
                           outputComparisons]);     # 3: output comparisons
                           

            total_used_budget += used_budget;
            total_comparisons += outputComparisons;
            
            # If the oldest workload is finished remove the request
            if self.queue_ks[0][1] <= 0:
                # Remove the oldest element in the queue
                self.queue_ks.popleft();

        # NOTE: This is overriding the number of total output comparisons
        # This is an easy-fix for the overloaded case. The Two quantities
        # give the same number in the non-overloaded case
        
        # Alternatively, one can just compute it as: total_used_budget/(self.alpha + self.sigma * self.beta)
        self.y = total_comparisons;


        if total_used_budget > 0.0:
            # Compute actual latency as
            # (delay + latency) * percentage of used budget 
            partial_latencies = [(x[1] + x[2]) * x[0]/total_used_budget for x in served]
            self.latency_overload = sum(partial_latencies)
        else:
            self.latency_overload = 0.0

    # Set the value of the timing properties of the Join
    def setTimingParameters(self, alpha, beta, sigma):
        self.alpha = alpha
        self.beta  = beta
        self.sigma = sigma
        self.apsb  = alpha + sigma * beta

    # Set the value of alpha
    def setAlpha(self, new_alpha):
        self.alpha = new_alpha

    # Set the value of alpha
    def setBeta(self, new_beta):
        self.beta = new_beta

    # Set the value of alpha
    def setSigma(self, new_sigma):
        self.sigma = new_sigma

    # Compute the next step for the simulation
    def computeNextStep(self, r, s, alpha, beta, sigma, n = None, w = None):
        self.curr_time = self.curr_time + 1;

        if n is not None: # The number of processing units changed:
            self.n = n;

        if w is not None: # The window size changed
            self.wR = w;
            self.wS = w;

        self.setTimingParameters(alpha,beta,sigma)  # updating the values of alpha, beta and sigma

        # Update steps
        self.updateTuples(r, s);        # updating the tuples to be compared
        self.updateComparisons();       # updating the number of comparisons
        self.updateCost();              # updating the time for computing all the comparisons
        self.updateComparisonOutput();  # updating the number of comparisons that will be output
        ell_parallelism,ell_join,ell_readyIn = \
            self.updateLatency();       # updating the latency
        self.updateQueueKs();           # updating the queue of ks

        # returning the state of the system
        return self.omegaR,\
               self.omegaS,\
               self.C,\
               self.k,\
               self.y,\
               self.latency_overload,\
               ell_parallelism,\
               ell_join,\
               ell_readyIn




