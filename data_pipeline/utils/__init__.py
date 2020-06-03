

def update_deployment_env_placeholder(
        original_dict: dict,
        deployment_env: str,
        environment_placeholder: str,
):
    new_dict = dict()
    for key, val in original_dict.items():
        if isinstance(val, dict):
            tmp = update_deployment_env_placeholder(
                val,
                deployment_env,
                environment_placeholder
            )
            new_dict[key] = tmp
        elif isinstance(val, list):
            updated_val = []
            for elem in val:
                if isinstance(elem, dict):
                    updated_val.append(
                        update_deployment_env_placeholder(
                            elem,
                            deployment_env,
                            environment_placeholder
                        )
                    )
                else:
                    updated_val.append(
                        replace_env_placeholder(
                            elem,
                            deployment_env,
                            environment_placeholder
                        )
                    )
            new_dict[key] = updated_val
        else:
            new_dict[key] = replace_env_placeholder(
                original_dict[key],
                deployment_env,
                environment_placeholder
            )
    return new_dict


def replace_env_placeholder(
        param_value,
        deployment_env: str,
        environment_placeholder: str
):
    new_value = param_value
    if isinstance(param_value, str):
        new_value = param_value.replace(
            environment_placeholder,
            deployment_env
        )
    return new_value
