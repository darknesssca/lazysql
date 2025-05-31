package components

import (
	"fmt"
	"strings"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/jorgerojas26/lazysql/app"
	"github.com/jorgerojas26/lazysql/commands"
	"github.com/jorgerojas26/lazysql/drivers"
	"github.com/jorgerojas26/lazysql/helpers"
	"github.com/jorgerojas26/lazysql/models"
)

type ConnectionSelection struct {
	*tview.Flex
	StatusText *tview.TextView
}

func NewConnectionSelection(connectionForm *ConnectionForm, connectionPages *models.ConnectionPages) *ConnectionSelection {
	wrapper := tview.NewFlex()

	wrapper.SetDirection(tview.FlexColumnCSS)

	buttonsWrapper := tview.NewFlex().SetDirection(tview.FlexRowCSS)

	newButton := tview.NewButton("[yellow]N[dark]ew")
	newButton.SetStyle(tcell.StyleDefault.Background(app.Styles.PrimitiveBackgroundColor))
	newButton.SetBorder(true)

	buttonsWrapper.AddItem(newButton, 0, 1, false)
	buttonsWrapper.AddItem(nil, 1, 0, false)

	connectButton := tview.NewButton("[yellow]C[dark]onnect")
	connectButton.SetStyle(tcell.StyleDefault.Background(app.Styles.PrimitiveBackgroundColor))
	connectButton.SetBorder(true)

	buttonsWrapper.AddItem(connectButton, 0, 1, false)
	buttonsWrapper.AddItem(nil, 1, 0, false)

	editButton := tview.NewButton("[yellow]E[dark]dit")
	editButton.SetStyle(tcell.StyleDefault.Background(app.Styles.PrimitiveBackgroundColor))
	editButton.SetBorder(true)

	buttonsWrapper.AddItem(editButton, 0, 1, false)
	buttonsWrapper.AddItem(nil, 1, 0, false)

	deleteButton := tview.NewButton("[yellow]D[dark]elete")
	deleteButton.SetStyle(tcell.StyleDefault.Background(app.Styles.PrimitiveBackgroundColor))
	deleteButton.SetBorder(true)

	buttonsWrapper.AddItem(deleteButton, 0, 1, false)
	buttonsWrapper.AddItem(nil, 1, 0, false)

	quitButton := tview.NewButton("[yellow]Q[dark]uit")
	quitButton.SetStyle(tcell.StyleDefault.Background(app.Styles.PrimitiveBackgroundColor))
	quitButton.SetBorder(true)

	buttonsWrapper.AddItem(quitButton, 0, 1, false)
	buttonsWrapper.AddItem(nil, 1, 0, false)

	statusText := tview.NewTextView()
	statusText.SetBorderPadding(1, 1, 0, 0)

	wrapper.AddItem(NewConnectionsTable(), 0, 1, true)
	wrapper.AddItem(statusText, 4, 0, false)
	wrapper.AddItem(buttonsWrapper, 3, 0, false)

	cs := &ConnectionSelection{
		Flex:       wrapper,
		StatusText: statusText,
	}

	wrapper.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		connections := connectionsTable.GetConnections()

		command := app.Keymaps.Group(app.ConnectionGroup).Resolve(event)

		if len(connections) != 0 {
			row, _ := connectionsTable.GetSelection()
			selectedConnection := connections[row]

			switch command {
			case commands.Connect:
				go cs.Connect(selectedConnection)
			case commands.EditConnection:
				connectionPages.SwitchToPage(pageNameConnectionForm)
				connectionForm.GetFormItemByLabel("Name").(*tview.InputField).SetText(selectedConnection.Name)
				connectionForm.GetFormItemByLabel("URL").(*tview.InputField).SetText(selectedConnection.URL)
				connectionForm.StatusText.SetText("")

				connectionForm.SetAction(actionEditConnection)
				return nil
			case commands.DeleteConnection:
				confirmationModal := NewConfirmationModal("")

				confirmationModal.SetDoneFunc(func(_ int, buttonLabel string) {
					mainPages.RemovePage(pageNameConfirmation)
					confirmationModal = nil

					if buttonLabel == "Yes" {
						newConnections := append(connections[:row], connections[row+1:]...)

						err := app.App.SaveConnections(newConnections)
						if err != nil {
							connectionsTable.SetError(err)
						} else {
							connectionsTable.SetConnections(newConnections)
						}

					}
				})

				mainPages.AddPage(pageNameConfirmation, confirmationModal, true, true)

				return nil
			}
		}

		switch command {
		case commands.NewConnection:
			connectionForm.SetAction(actionNewConnection)
			connectionForm.GetFormItemByLabel("Name").(*tview.InputField).SetText("")
			connectionForm.GetFormItemByLabel("URL").(*tview.InputField).SetText("")
			connectionForm.StatusText.SetText("")
			connectionPages.SwitchToPage(pageNameConnectionForm)
		case commands.Quit:
			if wrapper.HasFocus() {
				app.App.Stop()
			}
		}

		return event
	})

	return cs
}

func (cs *ConnectionSelection) Connect(connection models.Connection) *tview.Application {
	if mainPages.HasPage(connection.Name) {
		mainPages.SwitchToPage(connection.Name)
		return App.Draw()
	}

	if len(connection.Commands) > 0 {
		port, err := helpers.GetFreePort()
		if err != nil {
			cs.StatusText.SetText(err.Error()).SetTextStyle(tcell.StyleDefault.Foreground(tcell.ColorRed))
			return App.Draw()
		}

		// Replace ${port} with the actual port.
		connection.URL = strings.ReplaceAll(connection.URL, "${port}", port)

		for i, command := range connection.Commands {
			message := fmt.Sprintf("Running command %d/%d...", i+1, len(connection.Commands))
			cs.StatusText.SetText(message).SetTextColor(app.Styles.TertiaryTextColor)
			App.Draw()

			cmd := strings.ReplaceAll(command.Command, "${port}", port)
			if err := helpers.RunCommand(App.Context(), cmd, App.Register()); err != nil {
				cs.StatusText.SetText(err.Error()).SetTextStyle(tcell.StyleDefault.Foreground(tcell.ColorRed))
				return App.Draw()
			}

			if command.WaitForPort != "" {
				port := strings.ReplaceAll(command.WaitForPort, "${port}", port)

				message := fmt.Sprintf("Waiting for port %s...", port)
				cs.StatusText.SetText(message).SetTextColor(app.Styles.TertiaryTextColor)
				App.Draw()

				if err := helpers.WaitForPort(App.Context(), port); err != nil {
					cs.StatusText.SetText(err.Error()).SetTextStyle(tcell.StyleDefault.Foreground(tcell.ColorRed))
					return App.Draw()
				}
			}
		}
	}

	cs.StatusText.SetText("Connecting...").SetTextColor(app.Styles.TertiaryTextColor)
	App.Draw()

	var newDBDriver drivers.Driver

	switch connection.Provider {
	case drivers.DriverMySQL:
		newDBDriver = &drivers.MySQL{}
	case drivers.DriverPostgres:
		newDBDriver = &drivers.Postgres{}
	case drivers.DriverSqlite:
		newDBDriver = &drivers.SQLite{}
	case drivers.DriverMSSQL:
		newDBDriver = &drivers.MSSQL{}
	case drivers.DriverClickhouse:
		newDBDriver = &drivers.Clickhouse{}
	}

	err := newDBDriver.Connect(connection.URL)
	if err != nil {
		cs.StatusText.SetText(err.Error()).SetTextStyle(tcell.StyleDefault.Foreground(tcell.ColorRed))
		return App.Draw()
	}

	selectedRow, selectedCol := connectionsTable.GetSelection()
	cell := connectionsTable.GetCell(selectedRow, selectedCol)
	cell.SetText(fmt.Sprintf("[green]* %s", cell.Text))
	cs.StatusText.SetText("")

	newHome := NewHomePage(connection, newDBDriver)
	newHome.Tree.SetCurrentNode(newHome.Tree.GetRoot())
	newHome.Tree.Wrapper.SetTitle(connection.Name)

	mainPages.AddAndSwitchToPage(connection.Name, newHome, true)
	App.SetFocus(newHome.Tree)

	return App.Draw()
}
