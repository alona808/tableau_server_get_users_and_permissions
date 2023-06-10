
import pandas as pd
import logging as log

# Tableau API connection
import tableauserverclient as TSC


def connect_to_server_site(server_url, token_name, token_value, site_name):
    """
    Function connects to Tableau server site and returns a Tableau server object
    """
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_name)
    
    # Make sure we use an updated version of the rest apis, and pass in our cert handling choice
    check_ssl_certificate = False
    server = TSC.Server(server_url, use_server_version=True, 
                        http_options={"verify": check_ssl_certificate}) 
    server.auth.sign_in(tableau_auth)
    log.info(f'log: connected to Tableau server site', site_name)
    return server



def get_all_server_users(server):
    """
    Function switches between all Tableau server sites, 
    calls convert_user_list_to_df(get_tableau_site_users())
    and returns a pandas DataFrame of all Tableau server users
    """
    df = pd.DataFrame()
    for site in TSC.Pager(server.sites):
        server.auth.switch_site(site)
        log.info(f'log: we are here:', site.name)
        df = df.append(convert_user_list_to_df(server, site.name))
        log.info(f'log: Tableau server users of {site.name} site added to DataFrame')
    
    return df


def convert_user_list_to_df(server, site_name):
    """
    Function converts a list of tuples of Tableau site users to a pandas DataFrame and returns it
    """
    df = pd.DataFrame(get_tableau_site_users(server, site_name), 
                                   columns=['user_tableau_id'
                                            ,'user_name_ad'
                                            ,'server_email'
                                            ,'domain_name'
                                            ,'full_name'
                                            ,'site_role'
                                            ,'last_login'])
    df['last_login'] = df['last_login'].apply(lambda t: pd.to_datetime(t).date)
    df['site_name'] = site_name

    return df


def get_tableau_site_users(server, site_name):
    """
    Function returns a list of all Tableau site users and returns a list of tuples
    """
    lst = []
    for user in TSC.Pager(server.users.get):
        lst.append(
            ( user.id
             ,user.name.lower().strip()  if user.name and isinstance(user.name,str) else None
             ,user.email.lower().strip() if user.email and isinstance(user.email,str) else None
             ,user.domain_name
             ,user.fullname
             ,user.site_role
             ,user.last_login
            ))
    log.info(f'log: Tableau users loaded from the site', site_name)
    return lst


def get_all_server_groups(server):
    """
    Function switches between all Tableau server sites,
    calls get_tableau_site_groups(),
    appends data from each site to a pandas DataFrame and returns it
    """
    df = pd.DataFrame()
    for site in TSC.Pager(server.sites):
        server.auth.switch_site(site)
        log.info(f'log: we are here:', site.name)
        df = df.append(get_tableau_site_groups(site.name))
        log.info(f'log: Tableau server groups of {site.name} site added to DataFrame')
        
    return df


def get_tableau_site_groups(server, site_name):
    """
    The function collects from site all groups and users belonging to those groups
    and returns a pandas DataFrame
    """
    dict_group_users = {'group_id': [] 
                        ,'group_name': [] 
                        ,'group_domain_name': [] 
                        ,'group_min_site_role': [] 
                        ,'user_name_ad': [] 
                        ,'user_tableau_id': []
                        ,'site_name': []
                       }
    
    all_groups, pagination_item = server.groups.get()

    for group in all_groups :
        pagination_item = server.groups.populate_users(group,req_options=None)

        for user in group.users:
            dict_group_users['group_id'].append(group.id)
            dict_group_users['group_name'].append(group.name)
            dict_group_users['group_domain_name'].append(group.domain_name)
            dict_group_users['group_min_site_role'].append(group.minimum_site_role)        
            dict_group_users['user_tableau_id'].append(user.id)
            dict_group_users['user_name_ad'].append(user.name)
            dict_group_users['site_name'].append(site_name)  
    
    return pd.DataFrame(dict_group_users)


# Functions to get user permissions per project from Tableau server

def get_all_server_permissions(server):
    """
    Function switches between all Tableau server sites,
    calls convert_permissions_list_to_df(),
    appends data from each site to a pandas DataFrame and returns it
    """
    df = pd.DataFrame()
    for site in TSC.Pager(server.sites):
        server.auth.switch_site(site)
        log.info(f'log: we are here:', site.name)
        df = df.append(convert_permissions_list_to_df(server, site.name))
        log.info(f'log: Tableau server permissions of {site.name} site added to DataFrame')
        
    return df


def convert_permissions_list_to_df(server, site_name):
    """
    Function do the conversion of a list of tuples of Tableau site users to a pandas DataFrame and returns it
    """
    df = pd.DataFrame(get_user_site_permissions(server),
                      columns=['project_name', 'user_type', 'user_name_ad', 'user_permissions'])
    df['site_name'] = site_name

    return df


def get_user_site_permissions(server):
    """
    Function collects from site all permissions defined for projects and returns a list of tuples
    """
    lst = []

    all_projects, pagination_item = server.projects.get()
    for project in all_projects:
        server.projects.populate_permissions(project)
        permissions = project.permissions

        i = -1
        for rule in permissions:
            i += 1
            group_user_type = permissions[i].grantee.tag_name
            group_user_id = permissions[i].grantee.id
            group_user_capabilities = permissions[i].capabilities
            # user_tableau_id = permissions[i].id
            if group_user_type == 'user':
                user_item = server.users.get_by_id(permissions[i].grantee.id)
                group_user_name = user_item.name
            elif group_user_type == 'group':
                for group_item in TSC.Pager(server.groups):
                    if group_item.id == group_user_id:
                        group_user_name = group_item.name
                        break
                    
            _tpl = project.name, group_user_type, group_user_name, group_user_capabilities #, user_tableau_id
            lst.append(_tpl)
           
    return lst


def sign_out_from_server(server):
    server.auth.sign_out()
    log.info(f'log: logged out from Tableau server')

