

import sys
import os
import math
import numpy as np
from scipy.optimize import curve_fit
from astropy.stats import binom_conf_interval


def is_this_spock():
    """
    Check if current system is spock or scotty
    """
    local_os = sys.platform
    local_os = local_os[:(min(3, len(local_os)))]

    path = os.getcwd()
    in_smb = path.find('smb') == -1
    in_usr_people = path.find('usr/people') == -1
    in_jukebox = path.find('jukebox') == -1

    isSpock = ((in_smb or in_usr_people or in_jukebox) and
        (not local_os.lower() == 'win') and
        (not local_os.lower() == 'dar'))

    return isSpock


def basic_dj_configuration(dj):
    dj.config['database.host'] = 'datajoint00.pni.princeton.edu'
    dj.config['enable_python_native_blobs'] = True

    if is_this_spock():
        ext_storage_location = '/mnt/bucket/u19_dj/external_dj_blobs'
    elif sys.platform == "darwin":
        ext_storage_location = '/Volumes/u19_dj/external_dj_blobs'
    elif sys.platform == "win32":
        ext_storage_location = '\\\\bucket.pni.princeton.edu\\u19_dj\\external_dj_blobs'
    elif sys.platform == "linux" or sys.platform == "linux2":
        ext_storage_location = '/mnt/u19_dj/external_dj_blobs'

    dj.config['stores'] = {
        'extstorage':
            {
                'location': ext_storage_location,
                'protocol': 'file'
            }
    }


def psychometrics_function(x, O, A, lambd, x0):
    return O + A/(1+np.exp(-(x-x0)/lambd))


def psychFit(deltaBins, numR, numL, choices):
    numRight = np.zeros(len(deltaBins))
    numTrials = np.zeros(len(deltaBins))
    trialDelta = np.zeros(len(deltaBins))
    phat = np.zeros(len(deltaBins))
    pci = np.zeros((2, len(deltaBins)))

    # Evidence variable
    nCues_RminusL = numR - numL
    # Correct deltaBin & trialBin to produce same result as Matlab psychFit
    deltaBins_search = deltaBins.astype(float) - 1.5
    trialBin = np.searchsorted(deltaBins_search, nCues_RminusL, side='right')
    trialBin -= 1;

    # Put into evidence bins all Trials with corresponding choices
    for iTrial in range(len(choices)):
        numTrials[trialBin[iTrial]] = numTrials[trialBin[iTrial]] + 1
        if choices[iTrial] == 2:
            numRight[trialBin[iTrial]] = numRight[trialBin[iTrial]] + 1

        trialDelta[trialBin[iTrial]] = trialDelta[trialBin[iTrial]] + nCues_RminusL[iTrial]

    with np.errstate(divide='ignore', invalid='ignore'):
        trialDelta = np.true_divide(trialDelta, numTrials);

    # Select only bins with trials
    idx_zero = numTrials == 0
    numTrials_nz = numTrials[~idx_zero]
    numRight_nz = numRight[~idx_zero]

    # (Binomial proportion confidence interval given k successes, n trials)
    phat_nz = binom_conf_interval(numRight_nz, numTrials_nz, confidence_level=0, interval='jeffreys')
    pci_nz = binom_conf_interval(numRight_nz, numTrials_nz, confidence_level=1 - 0.1587, interval='jeffreys')

    # Correct confidence intervals and expected outcomes for bins with no trials (ci = [0 1], hat = 0.5)
    phat_nz = phat_nz[0]
    phat[~idx_zero] = phat_nz
    phat[idx_zero] = 0.5
    pci[0][~idx_zero] = pci_nz[0]
    pci[0][idx_zero] = 0
    pci[1][~idx_zero] = pci_nz[1]
    pci[1][idx_zero] = 1

    # (Logistic function fit) only valid if we have at least 5 bins with trials
    if np.count_nonzero(~idx_zero) < 5:
        is_there_psychometric = False
    else:
        is_there_psychometric = True
        # Get weight matrix to "reproduce" Matlab fit
        # https://stackoverflow.com/questions/58983113/scipy-curve-fit-vs-matlab-fit-weighted-nonlinear-least-squares
        # matlab -> 'Weights'         , ((pci(sel,2) - pci(sel,1))/2).^-2
        # python -> sigma = diagonal_matrix(1/weights)

        weight_array = np.power((pci[1][~idx_zero] - pci[0][~idx_zero]) / 2, 2)
        sigma_fit = np.diag(weight_array)

        psychometric, pcov = curve_fit(psychometrics_function, deltaBins[~idx_zero], phat[~idx_zero], \
                                       p0=(0, 1, 3, 0), sigma=sigma_fit, maxfev=40000)

    # Append a row of nans to confidence intervals . whyy ??
    aux_vec = np.empty((1, pci.shape[1]))
    aux_vec[:] = np.nan
    pci = np.vstack((pci, aux_vec))

    # x vector for plotting
    delta = np.linspace(deltaBins[0] - 2, deltaBins[-1] + 2, num=50)

    # Repeat trialDelta 3 times for errorX why ??
    errorX = np.tile(trialDelta[~idx_zero], 3);

    # Confidence intervals are errorY, as a vector
    errorY = np.stack(pci[:, ~idx_zero])
    errorY = errorY.flatten()

    # Fill  dictionary of results
    fit_results = dict()
    fit_results['delta_bins'] = deltaBins[~idx_zero]
    fit_results['delta_data'] = trialDelta[~idx_zero];
    fit_results['pright_data'] = 100 * phat[~idx_zero];
    fit_results['delta_error'] = errorX;
    fit_results['pright_error'] = 100 * errorY;

    if is_there_psychometric:
        fit_results['delta_fit'] = delta
        fit_results['pright_fit'] = psychometrics_function(delta, *psychometric) * 100
    else:
        fit_results['delta_fit'] = np.empty([0])
        fit_results['pright_fit'] = np.empty([0])

    return fit_results


def translate_choice_trials_cues(session_df):
    """
    Transform to numeric values trialtype, choice and left & right cues
    """

    session_df['trial_type_int'] = 0
    session_df.loc[session_df['trial_type'] == 'L','trial_type_int'] = 1
    session_df.loc[session_df['trial_type'] == 'R','trial_type_int'] = 2
    session_df['choice_int'] = 0
    session_df.loc[session_df['choice'] == 'L','choice_int'] = 1
    session_df.loc[session_df['choice'] == 'R','choice_int'] = 2

    # If we are translating towers task cues
    if 'cue_presence_left' in session_df.columns:
        session_df['cue_presence_left'] = session_df['cue_presence_left'].apply(lambda x: np.count_nonzero(x))
        session_df['cue_presence_right'] = session_df['cue_presence_right'].apply(lambda x: np.count_nonzero(x))
    # If we are translating puff task cues
    elif 'num_puffs_received_r' in session_df.columns:
        session_df['cue_presence_left'] = session_df['num_puffs_received_l']
        session_df['cue_presence_right'] = session_df['num_puffs_received_r']

    return session_df


def get_cols_rows_plot(num_plots, fig_size):

    fig_rel = fig_size[1] / fig_size[0]
    num_rows = math.floor(math.sqrt(num_plots))
    num_cols = num_rows
    while 1:

        ac_rel = num_cols / num_rows

        if num_cols * num_rows >= num_plots:
            break

        if ac_rel < fig_rel:
            num_rows += 1
        else:
            num_cols += 1

    return num_rows, num_cols

