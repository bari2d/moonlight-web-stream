import { Component, ComponentEvent } from "../index.js";
import { Api, apiDeleteUser, apiGetRoles, apiPatchUser } from "../../api.js";
import { DetailedUser, PatchUserRequest } from "../../api_bindings.js";
import { getCurrentLanguage, getTranslations } from "../../i18n.js";
import { InputComponent, SelectComponent } from "../input.js";
import { createSelectRoleInput } from "./role_select.js";
import { tryDeleteUser } from "./index.js";
import { showNotification } from "../notification.js";

export type DetailedUserPageEventListener = (event: ComponentEvent<DetailedUserPage>) => void

export class DetailedUserPage implements Component {

    private api: Api

    private formRoot = document.createElement("form")

    private id

    private idElement: InputComponent
    private name: InputComponent
    private password: InputComponent
    private role: SelectComponent
    private clientUniqueId: InputComponent

    private applyButton = document.createElement("button")
    private deleteButton = document.createElement("button")

    private rolesLoaded = false
    private saving = false
    private disposed = false

    private readonly deleteListener = () => {
        void this.delete()
    }
    private readonly submitListener = (event: SubmitEvent) => {
        void this.apply(event)
    }

    constructor(api: Api, user: DetailedUser) {
        this.api = api
        this.id = user.id
        const i = getTranslations(getCurrentLanguage()).admin

        this.formRoot.classList.add("user-info")

        this.idElement = new InputComponent("userId", "number", i.userId, {
            defaultValue: `${user.id}`
        })
        this.idElement.setEnabled(false)
        this.idElement.mount(this.formRoot)

        this.name = new InputComponent("userName", "text", i.userName, {
            defaultValue: user.name,
        })
        this.name.setEnabled(false)
        this.name.mount(this.formRoot)

        this.password = new InputComponent("userPassword", "text", i.password, {
            placeholer: i.newPassword,
            formRequired: true,
            hasEnableCheckbox: true
        })
        this.password.setEnabled(false)
        this.password.mount(this.formRoot)

        this.role = createSelectRoleInput([], user.role_id)
        this.role.mount(this.formRoot)
        this.applyButton.disabled = true
        apiGetRoles(api)
            .then(roles => {
                if (this.disposed) {
                    return
                }

                this.role.unmount(this.formRoot)

                this.role = createSelectRoleInput(roles.roles, user.role_id)
                this.role.mountBefore(this.formRoot, this.clientUniqueId)
                this.rolesLoaded = true
                this.updateButtonState()
            })
            .catch(error => {
                if (!this.disposed) {
                    showNotification(i.rolesLoadFailed, "error", error)
                }
            })

        this.clientUniqueId = new InputComponent("userClientUniqueId", "text", i.moonlightClientId, {
            defaultValue: user.client_unique_id,
        })
        this.clientUniqueId.mount(this.formRoot)

        this.applyButton.innerText = i.apply
        this.applyButton.type = "submit"
        this.formRoot.appendChild(this.applyButton)

        this.deleteButton.addEventListener("click", this.deleteListener)
        this.deleteButton.classList.add("user-info-delete")
        this.deleteButton.innerText = i.delete
        this.deleteButton.type = "button"
        this.formRoot.appendChild(this.deleteButton)

        this.formRoot.addEventListener("submit", this.submitListener)
    }

    private updateButtonState() {
        this.applyButton.disabled = !this.rolesLoaded || this.saving
        this.deleteButton.disabled = this.saving
    }

    private async apply(event: SubmitEvent) {
        event.preventDefault()
        const i = getTranslations(getCurrentLanguage()).admin

        if (!this.rolesLoaded || this.saving) {
            return
        }

        let password = null
        if (this.password.isEnabled()) {
            password = this.password.getValue()
        }

        const role = this.role.getValue()
        if (!role) {
            showNotification(i.pleaseSelectRole)
            return
        }

        const request: PatchUserRequest = {
            id: this.id,
            role_id: parseInt(role),
            password,
            client_unique_id: this.clientUniqueId.getValue()
        };

        this.saving = true
        this.updateButtonState()

        try {
            await apiPatchUser(this.api, request)

            if (!this.disposed) {
                // Do not accidentally resend a password on a later settings-only save.
                this.password.reset()
                this.password.setEnabled(false)
                showNotification(i.userUpdated, "info")
            }
        } catch (error) {
            if (!this.disposed) {
                showNotification(i.userUpdateFailed, "error", error)
            }
        } finally {
            this.saving = false
            if (!this.disposed) {
                this.updateButtonState()
            }
        }
    }

    private async delete() {
        await tryDeleteUser(this.api, this.id)

        this.formRoot.dispatchEvent(new ComponentEvent("ml-userdeleted", this))
    }

    addDeletedListener(listener: DetailedUserPageEventListener, options?: EventListenerOptions) {
        this.formRoot.addEventListener("ml-userdeleted", listener as any, options)
    }
    removeDeletedListener(listener: DetailedUserPageEventListener) {
        this.formRoot.removeEventListener("ml-userdeleted", listener as any)
    }

    getUserId(): number {
        return this.id
    }

    mount(parent: HTMLElement): void {
        parent.appendChild(this.formRoot)
    }
    unmount(parent: HTMLElement): void {
        this.disposed = true
        this.deleteButton.removeEventListener("click", this.deleteListener)
        this.formRoot.removeEventListener("submit", this.submitListener)
        parent.removeChild(this.formRoot)
    }
}
