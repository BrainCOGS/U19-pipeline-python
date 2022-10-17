
import pandas as pd
import datajoint as dj


class BehaviorMetrics():

    @staticmethod
    def get_bias_from_trial_df(trials_df, return_all_metrics=False):
        '''
        From a trial by trial df of multiple sessions, calculate a per session bias
        '''

        original_columns = trials_df.columns.tolist()
        sort_columns = ['subject_fullname', 'session_date', 'session_number', 'block', 'trial_idx']
        session_columns = ['subject_fullname', 'session_date', 'session_number']
        trials_df = trials_df.sort_values(by=sort_columns, ascending=[True, False, False, True, True])
        trials_df = trials_df.reset_index(drop=True)

        # Left & right trials as integers
        trials_df['left_trial'] = (trials_df['trial_type'] == 'L').astype(int)
        trials_df['right_trial'] = (trials_df['trial_type'] == 'R').astype(int)
        trials_df['trial'] = 1

        # Cumulative sum of Left & right trials as integers per session
        trials_df['cum_left_trials'] = trials_df.groupby(session_columns)['left_trial'].cumsum()
        trials_df['cum_right_trials'] = trials_df.groupby(session_columns)['right_trial'].cumsum()
        trials_df['cum_trials'] = trials_df.groupby(session_columns)['trial'].cumsum()

        # Correct trials per side as integers
        trials_df['correct_trial'] = (trials_df['trial_type'] == trials_df['choice']).astype(int)
        trials_df['correct_left'] = ((trials_df['correct_trial'] == 1) & (trials_df['trial_type'] == 'L')).astype(int)
        trials_df['correct_right'] = ((trials_df['correct_trial'] == 1) & (trials_df['trial_type'] == 'R')).astype(int)

        # Cumulative sum of per side correct trials
        trials_df['cum_correct_left_trials'] = trials_df.groupby(session_columns)['correct_left'].cumsum()
        trials_df['cum_correct_right_trials'] = trials_df.groupby(session_columns)['correct_right'].cumsum()
        trials_df['cum_correct_trials'] = trials_df.groupby(session_columns)['correct_trial'].cumsum()

        # Get only last trial count
        trials_df = trials_df.loc[~trials_df.duplicated(subset=session_columns, keep='last'), :]
        trials_df = trials_df.reset_index(drop=True)

        # Calculate bias
        trials_df['bias'] = (trials_df['cum_correct_right_trials'] / trials_df['cum_right_trials']) - (trials_df['cum_correct_left_trials'] / trials_df['cum_left_trials'])
        
        if return_all_metrics:
            bias_df = trials_df
        else:
            bias_df = trials_df[original_columns + ['bias']]

        return bias_df
        

    @staticmethod
    def get_zscore_metric_session_df(session_df, metric, groupby_column):
        '''
        Get zscore from a "generic" metric of a session dataframe
        '''

        #Sort by subject, date and session number
        session_df = session_df.sort_values(by=['subject_fullname', 'session_date', 'session_number'], ascending=[True, False, False])
        session_df = session_df.reset_index(drop=True)

        # Lables for avg an std metric columns 
        avg_metric_l = 'avg_'+ metric
        std_metric_l = 'std_'+metric
        z_score_metric_l = 'z_score_'+metric

        # Get mean and std for the selected metric
        avg_df = session_df.groupby(groupby_column).agg({metric: [(avg_metric_l, 'mean'), (std_metric_l, 'std')]})
        avg_df.columns = avg_df.columns.droplevel()
        avg_df = avg_df.reset_index()

        # Merge average with session _df
        session_df = session_df.merge(avg_df)

        # Calculate proper zscore
        session_df[z_score_metric_l] = (session_df[metric] - session_df[avg_metric_l]) / session_df[std_metric_l]

        return session_df