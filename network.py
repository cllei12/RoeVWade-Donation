# Two-mode network 
import pandas as pd
from tqdm import tqdm

def build_two_mode_network(df: pd.DataFrame, tie_colname: str, id_colname: str, 
                           is_wrighted: bool = True, is_bidirectional: bool = True) -> pd.DataFrame:
    """
    Build a two-mode network from the input DataFrame.
    
    Parameters:
        df (pd.DataFrame): Input DataFrame containing user IDs and zip codes.
        tie_colname (str): The column name of the ties (e.g., zip codes 'ZIP').
        id_colname (str): The column name of the user IDs. (e.g., person IDs 'PID').
        is_wrighted (bool): Whether to add edge weights to the network. Default is True.
        is_bidirectional (bool): Whether the network is bidirectional. Default is True.
    
    Returns:
        (pd.DataFrame): A DataFrame of the two-mode network: source, dest, weight (optional).
    """
    # Group data by zip_code and select groups with size greater than 1 (ignore isolated nodes)
    duplicate_zip_groups = df.groupby(tie_colname).filter(lambda x: len(x) > 1)
    # Extract the user IDs sharing the same zip code
    duplicate_users = duplicate_zip_groups.groupby(tie_colname)[id_colname].apply(list)

    # Initialize an empty list to store pairs of users who share a zip code
    edge_list = []
    # Initialize tqdm progress bar with the total number of zip codes
    progress_bar = tqdm(total=len(duplicate_users), desc="Processing")
    # Iterate through the groups to link pairs of users sharing the same zip code
    for zip_code, users in duplicate_users.items():
        for i in range(len(users)):
            for j in range(i+1, len(users)):
                if users[i] != users[j]:  # Ignore self-linked nodes, not useful for SV calculation
                    edge_list.append((users[i], users[j]))
        # Update progress bar after processing each zip code
        progress_bar.update(1)
    # Close the progress bar
    progress_bar.close()
    
    # Create a DataFrame from the edge list
    two_mode_network = pd.DataFrame(edge_list, columns=['source', 'dest'])
    
    # Add edge weights if required
    if is_wrighted:
        two_mode_network['weight'] = 1
    # Duplicate the edges in the opposite direction if the network is bidirectional
    if is_bidirectional:
        two_mode_network = pd.concat([
            two_mode_network, 
            two_mode_network.rename(columns={'source': 'dest', 'dest': 'source'})
        ])
    
    return two_mode_network

    
# Test the function
if __name__ == '__main__':
    # Create a sample DataFrame
    data = {
        'PID': [1, 2, 3, 4, 5, 6],
        'ZIP': ['A', 'B', 'A', 'C', 'B', 'C']
    }
    df = pd.DataFrame(data)
    
    # Build the two-mode network
    two_mode_network = build_two_mode_network(df, tie_colname='ZIP', id_colname='PID')
    # Display the two-mode network
    print(two_mode_network)
