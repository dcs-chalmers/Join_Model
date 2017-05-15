#!/usr/bin/env python

import numpy as np
import argparse
import os
import sys
import csv
import logging

## Import my Libraries
import libs.utils as ut
import libs.parallelJoin as pj

def main():
    ## Manage command line inputs
    # Defining command line options to find out the algorithm
    parser = argparse.ArgumentParser( \
        description='Run Parallel Join simulator.', \
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--window',
        type = int,
        help = 'Window size (integer number).',
        default = 5)

    parser.add_argument('--n',
        type = int,
        help = 'Number of processing units (integer number).',
        default = 1)

    parser.add_argument('--determinism',
        type = int,
        help = 'Specifies if there is determinism. Binary value: 1 = determinism is enabled, 0 = determinism is disabled (integer number).',
        default = 1)

    parser.add_argument('--resultsFile',
        help = 'Filename where the results are stored.',
        default = 'results.csv')

    parser.add_argument('--inputFileAlpha',
        help = 'Filename where the data for the values of alpha.',
        default = 'inputData/trace_alpha.csv')

    parser.add_argument('--inputFileBeta',
        help = 'Filename where the data for the values of beta.',
        default = 'inputData/trace_beta.csv')

    parser.add_argument('--inputFileSigma',
        help = 'Filename where the data for the values of the selectivity sigma.',
        default = 'inputData/trace_sigma.csv')

    parser.add_argument('--inputFileR',
        help = 'Filename where the input stream for R is.',
        default = 'inputData/trace_r.csv')

    parser.add_argument('--inputFileS',
        help = 'Filename where the input stream for S is.',
        default = 'inputData/trace_s.csv')

    parser.add_argument('--showFig',
        type = int,
        help = 'Binary value: 1 = show the graph of the results, 0 = do not show the graph of the results',
        default = 1)

    parser.add_argument('--quota',
        type = float,
        help = 'Quota of time when overloaded',
        default = 1.0)

    parser.add_argument('--windowType',
        type = int,
        help = 'Type of the window: 0 = time-based windows, 1 = tuple-based windows (currently not supported) [MANDATORY argument]')


    # Print version
    parser.add_argument("--version", action="version", version='%(prog)s - Version 0.1')


    ## End management of command line inputs

    # Parsing the command line inputs
    args = parser.parse_args()


    # Setting logging information
    filename = args.resultsFile
    FORMAT = '%(message)s';
    logging.basicConfig(filename=filename,\
                        format=FORMAT,\
                        filemode='w',\
                        level=logging.INFO)
    logging.info('Time,r,s,n,window_size,omegaR,omegaS,C,k,y,latency,ell_readyIn,ell_join,ell_parallelism,alpha,beta,sigma')

    ## Reading the input data
    fn_alpha = args.inputFileAlpha
    fn_beta  = args.inputFileBeta
    fn_sigma = args.inputFileSigma
    fn_s = args.inputFileS
    fn_r = args.inputFileR

    t_alpha, vec_alpha, _ = ut.readInputData(fn_alpha)
    t_beta,  vec_beta, _  = ut.readInputData(fn_beta)
    t_sigma, vec_sigma, _ = ut.readInputData(fn_sigma)
    t_r, vec_r, num_r_streams = ut.readInputData(fn_r)
    t_s, vec_s, num_s_streams = ut.readInputData(fn_s)


    # Defining the maximum time and time resolution
    Tfin = min(len(t_r),len(t_s),len(t_alpha),len(t_beta),len(t_sigma))
    dt = t_r[1] - t_r[0]

    # Instatiating the processing units
    window_size = args.window; # Window size
    nu          = args.n;      # Number of processing units
    quota       = args.quota;  # Quota of time

    if args.determinism == 0:
        determinism = False
    else:
        determinism = True

    PUs = pj.ProcessingUnits(windowType = args.windowType, \
    	                     wR = window_size, \
    	                     wS = window_size, \
    	                     dt = dt, \
    	                     num_r = num_r_streams, \
    	                     num_s = num_s_streams, \
    	                     n_init = nu, \
    	                     w_max = 1000, \
    	                     determinism = determinism,
    	                     quota = quota);


    variables = np.zeros((11,Tfin))

    for kk in range(0,Tfin):
        # Simulation things
        ut.progress(kk,Tfin)   # Show progress bar

        # Reading the incoming arrival rate
    	r = vec_r[kk];    # Arrival rate for stream r
    	s = vec_s[kk];    # Arrival rate for stream s

    	alpha = vec_alpha[kk][0]; # Alpha
    	beta  = vec_beta[kk][0];  # Beta
    	sigma = vec_sigma[kk][0]; # Selectivity

        # Compute one step of the simulation
        omegaR,omegaS,C,k,y,latency,ell_readyIn,ell_join,ell_parallelism = PUs.computeNextStep(r, s, alpha, beta, sigma);

        if args.showFig:
            variables[0,kk] = sum(r);
            variables[1,kk] = sum(s);
            variables[2,kk] = omegaR;
            variables[3,kk] = omegaS;
            variables[4,kk] = C;
            variables[5,kk] = k;
            variables[6,kk] = y;
            variables[7,kk] = latency;
            variables[8,kk] = alpha;
            variables[9,kk] = beta;
            variables[10,kk] = sigma;

        # Updating the data on the log file
        logging.info('%d,%g,%g,%d,%d,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g',\
                     kk,\
                     sum(r),\
                     sum(s),\
                     nu,\
                     window_size,\
                     omegaR,\
                     omegaS,\
                     C,\
                     k,\
                     y,\
                     latency,\
                     ell_readyIn,\
                     ell_join,\
                     ell_parallelism,\
                     alpha,\
                     beta,\
                     sigma)

    print '\nSimulation completed!\n'

    ## Plotting results
    if args.showFig:
        time = np.matrix(range(0,Tfin)) * dt
        ynames = ['r','s','omegaR', 'omegaS', 'C', 'k', 'y', 'latency', 'alpha', 'beta', 'sigma']
        ut.plot_res_y(time,variables,ynames)


if __name__ == "__main__":
    sys.exit(main())