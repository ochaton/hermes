# hermes

Send a file or folder to another computer on the same network.

## Install

macOS / Linux:

    curl -fsSL https://raw.githubusercontent.com/ochaton/hermes/experiment/ux-send/install.sh | sh

Windows (PowerShell):

    irm https://raw.githubusercontent.com/ochaton/hermes/experiment/ux-send/install.ps1 | iex

## Use

    hermes send photo.jpg
    hermes get 7-river-lamp

Folders work the same way.

Agents: `hermes send --json <path>` and `hermes get --json <code>`.
Stdout events: ready, progress, done, error.
