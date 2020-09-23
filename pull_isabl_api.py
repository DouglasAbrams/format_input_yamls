import isabl_cli as ii
import pandas as pd
import click
import time


@click.group()
def cli():
    pass


def get_project_list(projects):
    titles = []
    for project_title in projects:
        titles.append(project_title.title)
    return ", ".join(titles)


def get_detailed_paths(analyses):
    data = []

    for analysis in analyses:
        for result in analysis.results:

            references = True if len(analysis.references) > 0 else False
            dfr = {
                'analysis_pk': analysis.pk,
                'project(s)': get_project_list(analysis.targets[0].projects),
                'individual': analysis.targets[0].sample.individual.identifier,
                'target_sample': analysis.targets[0].sample.identifier,
                'target_sample_category': analysis.targets[0].sample.category,
                'target_aliquot': analysis.targets[0].aliquot_id,
                'target_experiment': analysis.targets[0].system_id,
                'reference_sample': analysis.references[0].sample.identifier if references else None,
                'reference_sample_category': analysis.references[0].sample.category if references else None,
                'reference_aliquot': analysis.references[0].aliquot_id if references else None,
                'reference_experiment': analysis.references[0].system_id if references else None,
                'app': analysis.application.name,
                'app_version': analysis.application.version,
                'status': analysis.status,
                'file_type': result,
                'path': analysis.results[result],
            }
            data.append(dfr)

    return data


def get_parent_paths(analyses):
    data = []

    for analysis in analyses:
        references = True if len(analysis.references) > 0 else False
        dfr = {
            'analysis_pk': analysis.pk,
            'project(s)': get_project_list(analysis.targets[0].projects),
            'individual': analysis.targets[0].sample.individual.identifier,
            'target_sample': analysis.targets[0].sample.identifier,
            'target_sample_category': analysis.targets[0].sample.category,
            'target_aliquot': analysis.targets[0].aliquot_id,
            'target_experiment': analysis.targets[0].system_id,
            'reference_sample': analysis.references[0].sample.identifier if references else None,
            'reference_sample_category': analysis.references[0].sample.category if references else None,
            'reference_aliquot': analysis.references[0].aliquot_id if references else None,
            'reference_experiment': analysis.references[0].system_id if references else None,
            'app': analysis.application.name,
            'app_version': analysis.application.version,
            'status': analysis.status,
            'path': analysis.storage_url,
        }
        data.append(dfr)

    return data


def get_data(apps, project):
    """
    Retrieve analyses paths from Isabl.
    """

    analyses = ii.get_instances(
        'analyses',
        application__name__in=apps,
        projects__title__in=project
    )

    data = get_parent_paths(analyses)

    return pd.DataFrame(data)[["individual", "app", "target_sample_category", "target_sample", "path"]]



@cli.command()
@click.option('--apps', required=True, help='comma separated Isabl apps, use quotes when specifying multiple')
@click.option('--projects', required=True, help='comma separated Isabl projects, use quotes when specifying multiple')
@click.option('--details', is_flag=True, default=False, help='detailed list of files')
def get_paths(apps, projects, details):
    """
    Retrieve analyses paths from Isabl.
    """
    # apps = apps.replace(' ', '')
    # projects = projects.replace(' ', '')

    analyses = ii.get_instances(
        'analyses',
        application__name__in=apps,
        projects__title__in=projects
    )

    if details:
        data = get_detailed_paths(analyses)
    else:
        data = get_parent_paths(analyses)

    df = pd.DataFrame(data)
    df.to_csv(f'./isabl_paths_{time.strftime("%Y%m%d-%H%M%S")}.csv', index=False)


@cli.command()
def list_apps():
    """
    List apps currently in Isabl
    """
    apps = ii.get_instances('applications')

    data = []
    for app in apps:
        data.append({'app_name': app.name})

    df = pd.DataFrame(data).sort_values(by=['app_name'], ascending=True).drop_duplicates()
    for index, row in df.iterrows():
        print(row['app_name'])


@cli.command()
def list_projects():
    """
    List projects currently in Isabl
    """
    projects = ii.get_instances('projects')

    data = []
    for project in projects:
        data.append({'project_title': project.title})

    df = pd.DataFrame(data).sort_values(by=['project_title'], ascending=True)
    for index, row in df.iterrows():
        print(row['project_title'])



if __name__ == "__main__":
    cli()
